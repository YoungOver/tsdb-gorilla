// Package gorilla implements the time-series compression from
// "Gorilla: A Fast, Scalable, In-Memory Time Series Database" (Facebook, VLDB 2015):
// delta-of-delta timestamps and XOR-encoded float64 values.
package gorilla

import "errors"

var ErrShort = errors.New("gorilla: stream ended early")

// bwriter appends bits MSB-first into a byte slice.
type bwriter struct {
	b    []byte
	free uint8 // unused low bits in the last byte
}

func (w *bwriter) writeBit(bit bool) {
	if w.free == 0 {
		w.b = append(w.b, 0)
		w.free = 8
	}
	if bit {
		w.b[len(w.b)-1] |= 1 << (w.free - 1)
	}
	w.free--
}

// writeBits writes the low n bits of u, most significant first.
func (w *bwriter) writeBits(u uint64, n int) {
	u <<= 64 - uint(n)
	for n >= 8 {
		w.writeByte(byte(u >> 56))
		u <<= 8
		n -= 8
	}
	for n > 0 {
		w.writeBit(u>>63 == 1)
		u <<= 1
		n--
	}
}

func (w *bwriter) writeByte(c byte) {
	if w.free == 0 {
		w.b = append(w.b, c)
		return
	}
	w.b[len(w.b)-1] |= c >> (8 - w.free)
	w.b = append(w.b, c<<w.free)
}

// breader reads bits MSB-first.
type breader struct {
	b   []byte
	pos uint // bit position
}

func (r *breader) readBit() (bool, error) {
	if r.pos>>3 >= uint(len(r.b)) {
		return false, ErrShort
	}
	bit := r.b[r.pos>>3]>>(7-r.pos&7)&1 == 1
	r.pos++
	return bit, nil
}

func (r *breader) readBits(n int) (uint64, error) {
	if r.pos+uint(n) > uint(len(r.b))*8 {
		return 0, ErrShort
	}
	var u uint64
	for n > 0 {
		off := r.pos & 7
		avail := 8 - int(off)
		take := min(avail, n)
		cur := uint64(r.b[r.pos>>3]<<off) >> (8 - take)
		u = u<<uint(take) | cur
		r.pos += uint(take)
		n -= take
	}
	return u, nil
}
