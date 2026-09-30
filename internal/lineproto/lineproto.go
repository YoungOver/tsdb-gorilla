// Package lineproto parses a subset of the InfluxDB line protocol:
//
//	measurement[,tag=value...] field=number[,field=number...] [unix_ms]
//
// Each numeric field becomes its own series with a canonical key
// "measurement.field{tagA=x,tagB=y}" (tags sorted; field "value" is dropped from the name).
package lineproto

import (
	"bytes"
	"errors"
	"strconv"
	"unsafe"
)

var ErrSyntax = errors.New("lineproto: syntax error")

type Parser struct {
	key  []byte
	tags [][]byte
}

// Parse calls emit for every sample in buf. Malformed lines are skipped and counted.
// The key slice passed to emit is only valid during the call.
func (p *Parser) Parse(buf []byte, now int64, emit func(key []byte, t int64, v float64)) (ok, bad int) {
	for len(buf) > 0 {
		var line []byte
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			line, buf = buf[:i], buf[i+1:]
		} else {
			line, buf = buf, nil
		}
		line = bytes.TrimRight(line, "\r ")
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		n, err := p.parseLine(line, now, emit)
		if err != nil {
			bad++
			continue
		}
		ok += n
	}
	return
}

func (p *Parser) parseLine(line []byte, now int64, emit func([]byte, int64, float64)) (int, error) {
	sp1 := bytes.IndexByte(line, ' ')
	if sp1 <= 0 {
		return 0, ErrSyntax
	}
	head, rest := line[:sp1], line[sp1+1:]
	fields, tsPart := rest, []byte(nil)
	if sp2 := bytes.IndexByte(rest, ' '); sp2 >= 0 {
		fields, tsPart = rest[:sp2], bytes.TrimSpace(rest[sp2+1:])
	}
	t := now
	if len(tsPart) > 0 {
		v, err := strconv.ParseInt(b2s(tsPart), 10, 64)
		if err != nil {
			return 0, ErrSyntax
		}
		t = v
	}

	meas := head
	p.tags = p.tags[:0]
	if c := bytes.IndexByte(head, ','); c >= 0 {
		meas = head[:c]
		for rest := head[c+1:]; len(rest) > 0; {
			kv := rest
			if j := bytes.IndexByte(rest, ','); j >= 0 {
				kv, rest = rest[:j], rest[j+1:]
			} else {
				rest = nil
			}
			if bytes.IndexByte(kv, '=') <= 0 {
				return 0, ErrSyntax
			}
			p.tags = append(p.tags, kv)
		}
		// insertion sort: tag lists are short and usually already sorted, and it does not allocate
		for i := 1; i < len(p.tags); i++ {
			for j := i; j > 0 && bytes.Compare(p.tags[j], p.tags[j-1]) < 0; j-- {
				p.tags[j], p.tags[j-1] = p.tags[j-1], p.tags[j]
			}
		}
	}
	if len(meas) == 0 {
		return 0, ErrSyntax
	}

	n := 0
	for len(fields) > 0 {
		var f []byte
		if c := bytes.IndexByte(fields, ','); c >= 0 {
			f, fields = fields[:c], fields[c+1:]
		} else {
			f, fields = fields, nil
		}
		eq := bytes.IndexByte(f, '=')
		if eq <= 0 {
			return n, ErrSyntax
		}
		name, raw := f[:eq], bytes.TrimSuffix(f[eq+1:], []byte{'i'})
		v, err := strconv.ParseFloat(b2s(raw), 64)
		if err != nil {
			return n, ErrSyntax
		}
		p.key = append(p.key[:0], meas...)
		if string(name) != "value" {
			p.key = append(p.key, '.')
			p.key = append(p.key, name...)
		}
		if len(p.tags) > 0 {
			p.key = append(p.key, '{')
			for i, tg := range p.tags {
				if i > 0 {
					p.key = append(p.key, ',')
				}
				p.key = append(p.key, tg...)
			}
			p.key = append(p.key, '}')
		}
		emit(p.key, t, v)
		n++
	}
	return n, nil
}

// b2s views bytes as a string without copying; valid only while b is unchanged.
func b2s(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }
