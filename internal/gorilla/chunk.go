package gorilla

import (
	"math"
	"math/bits"
)

// Encoder appends (timestamp, value) pairs to a compressed chunk.
// Timestamps must be strictly increasing; callers enforce this.
type Encoder struct {
	w        bwriter
	n        int
	t, delta int64
	v        uint64
	leading  uint8
	trailing uint8
}

func NewEncoder() *Encoder { return &Encoder{leading: 0xff} }

func (e *Encoder) Len() int      { return e.n }
func (e *Encoder) Bytes() []byte { return e.w.b }
func (e *Encoder) LastT() int64  { return e.t }

func (e *Encoder) Append(t int64, v float64) {
	vb := math.Float64bits(v)
	switch e.n {
	case 0:
		e.w.writeBits(uint64(t), 64)
		e.w.writeBits(vb, 64)
	case 1:
		// first delta: 1 flag bit + 32 bits covers gaps up to ~24 days in ms
		delta := t - e.t
		if delta >= 0 && delta < 1<<32 {
			e.w.writeBit(false)
			e.w.writeBits(uint64(delta), 32)
		} else {
			e.w.writeBit(true)
			e.w.writeBits(uint64(delta), 64)
		}
		e.delta = delta
		e.writeXOR(vb)
	default:
		delta := t - e.t
		e.writeDoD(delta - e.delta)
		e.delta = delta
		e.writeXOR(vb)
	}
	e.t, e.v = t, vb
	e.n++
}

// Delta-of-delta buckets tuned for scrape-style data (regular intervals give dod == 0).
func (e *Encoder) writeDoD(dod int64) {
	switch {
	case dod == 0:
		e.w.writeBit(false)
	case fits(dod, 7):
		e.w.writeBits(0b10, 2)
		e.w.writeBits(uint64(dod), 7)
	case fits(dod, 9):
		e.w.writeBits(0b110, 3)
		e.w.writeBits(uint64(dod), 9)
	case fits(dod, 12):
		e.w.writeBits(0b1110, 4)
		e.w.writeBits(uint64(dod), 12)
	default:
		e.w.writeBits(0b1111, 4)
		e.w.writeBits(uint64(dod), 64)
	}
}

func fits(x int64, n uint) bool {
	lo := -(int64(1) << (n - 1)) + 1
	hi := int64(1) << (n - 1)
	return x >= lo && x <= hi
}

func (e *Encoder) writeXOR(vb uint64) {
	x := vb ^ e.v
	if x == 0 {
		e.w.writeBit(false)
		return
	}
	e.w.writeBit(true)
	lead := uint8(bits.LeadingZeros64(x))
	trail := uint8(bits.TrailingZeros64(x))
	if lead > 31 {
		lead = 31
	}
	if e.leading != 0xff && lead >= e.leading && trail >= e.trailing {
		e.w.writeBit(false)
		e.w.writeBits(x>>e.trailing, 64-int(e.leading)-int(e.trailing))
		return
	}
	e.leading, e.trailing = lead, trail
	sig := 64 - int(lead) - int(trail)
	e.w.writeBit(true)
	e.w.writeBits(uint64(lead), 5)
	e.w.writeBits(uint64(sig&63), 6) // 64 significant bits is stored as 0
	e.w.writeBits(x>>trail, sig)
}

// Iterator decodes a chunk produced by Encoder.
type Iterator struct {
	r        breader
	n, i     int
	t, delta int64
	v        uint64
	leading  uint8
	trailing uint8
	err      error
}

func NewIterator(b []byte, n int) *Iterator { return &Iterator{r: breader{b: b}, n: n} }

func (it *Iterator) At() (int64, float64) { return it.t, math.Float64frombits(it.v) }
func (it *Iterator) Err() error           { return it.err }

func (it *Iterator) Next() bool {
	if it.err != nil || it.i >= it.n {
		return false
	}
	if it.i == 0 {
		t, err := it.r.readBits(64)
		if err != nil {
			return it.fail(err)
		}
		v, err := it.r.readBits(64)
		if err != nil {
			return it.fail(err)
		}
		it.t, it.v = int64(t), v
		it.i++
		return true
	}
	if it.i == 1 {
		wide, err := it.r.readBit()
		if err != nil {
			return it.fail(err)
		}
		n := 32
		if wide {
			n = 64
		}
		d, err := it.r.readBits(n)
		if err != nil {
			return it.fail(err)
		}
		it.delta = int64(d)
	} else {
		dod, err := it.readDoD()
		if err != nil {
			return it.fail(err)
		}
		it.delta += dod
	}
	it.t += it.delta
	if err := it.readXOR(); err != nil {
		return it.fail(err)
	}
	it.i++
	return true
}

func (it *Iterator) fail(err error) bool { it.err = err; return false }

func (it *Iterator) readDoD() (int64, error) {
	var prefix int
	for prefix < 4 {
		b, err := it.r.readBit()
		if err != nil {
			return 0, err
		}
		if !b {
			break
		}
		prefix++
	}
	var n int
	switch prefix {
	case 0:
		return 0, nil
	case 1:
		n = 7
	case 2:
		n = 9
	case 3:
		n = 12
	default:
		n = 64
	}
	u, err := it.r.readBits(n)
	if err != nil {
		return 0, err
	}
	if n == 64 {
		return int64(u), nil
	}
	// sign-extend n-bit two's complement; the top positive value 2^(n-1) wraps negative
	x := int64(u)
	if u&(1<<(n-1)) != 0 {
		x = int64(u) - int64(1)<<n
		if x == -(int64(1) << (n - 1)) {
			x = int64(1) << (n - 1)
		}
	}
	return x, nil
}

func (it *Iterator) readXOR() error {
	same, err := it.r.readBit()
	if err != nil {
		return err
	}
	if !same {
		return nil
	}
	ctrl, err := it.r.readBit()
	if err != nil {
		return err
	}
	if ctrl {
		lead, err := it.r.readBits(5)
		if err != nil {
			return err
		}
		sig, err := it.r.readBits(6)
		if err != nil {
			return err
		}
		if sig == 0 {
			sig = 64
		}
		it.leading = uint8(lead)
		it.trailing = uint8(64 - lead - sig)
	}
	sig := 64 - int(it.leading) - int(it.trailing)
	x, err := it.r.readBits(sig)
	if err != nil {
		return err
	}
	it.v ^= x << it.trailing
	return nil
}
