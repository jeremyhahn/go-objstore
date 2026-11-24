// Package chunker splits a stream into content-defined chunks with FastCDC
// (Xia et al., "FastCDC: a Fast and Efficient Content-Defined Chunking
// Approach for Data Deduplication", USENIX ATC 2016).
//
// A boundary is placed where a Gear rolling hash of the bytes before it meets
// a mask, so boundaries follow the content: an insertion or deletion moves
// only the boundaries near it, and the chunks after it are found again. The
// chunker uses the paper's sub-minimum skipping and normalized chunking (level
// 2): a stricter mask before the average size and a looser one after it, which
// narrows the spread of chunk sizes.
//
// For a given input and Params the boundaries are always the same, however the
// reader splits its reads. The Gear table is fixed, so it is part of the
// repository format.
package chunker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"strconv"
)

// Default chunk sizes.
const (
	DefaultMin = 512 << 10
	DefaultAvg = 1 << 20
	DefaultMax = 8 << 20
)

// Bounds on Params.
const (
	// MinMin is the smallest Min allowed.
	MinMin = 64
	// MinAvg is the smallest Avg allowed.
	MinAvg = 256
	// MaxMax is the largest Max allowed.
	MaxMax = 256 << 20
)

// ErrInvalidParams is matched by every *ParamsError.
var ErrInvalidParams = errors.New("chunker: invalid parameters")

// ParamsError reports a Params field out of bounds.
type ParamsError struct {
	// Field is the offending field: "Min", "Avg" or "Max".
	Field string
	// Value is the field's value.
	Value int
	// Reason says which bound it breaks.
	Reason string
}

// Error describes the field, its value and the bound.
func (e *ParamsError) Error() string {
	return "chunker: " + e.Field + " " + strconv.Itoa(e.Value) + ": " + e.Reason
}

// Is reports whether target is ErrInvalidParams.
func (e *ParamsError) Is(target error) bool {
	return target == ErrInvalidParams
}

// Params are the chunk size bounds, in bytes. Every chunk but the last of a
// stream is at least Min and at most Max bytes; Avg is the size the chunker
// aims for. Avg must be a power of two with Min < Avg < Max.
type Params struct {
	Min int
	Avg int
	Max int
}

// DefaultParams returns 512 KiB, 1 MiB and 8 MiB.
func DefaultParams() Params {
	return Params{Min: DefaultMin, Avg: DefaultAvg, Max: DefaultMax}
}

// Validate returns a *ParamsError for the first field out of bounds, or nil.
func (p Params) Validate() error {
	switch {
	case p.Min < MinMin:
		return &ParamsError{Field: "Min", Value: p.Min, Reason: "below " + strconv.Itoa(MinMin)}
	case p.Avg < MinAvg:
		return &ParamsError{Field: "Avg", Value: p.Avg, Reason: "below " + strconv.Itoa(MinAvg)}
	case p.Avg&(p.Avg-1) != 0:
		return &ParamsError{Field: "Avg", Value: p.Avg, Reason: "not a power of two"}
	case p.Min >= p.Avg:
		return &ParamsError{Field: "Min", Value: p.Min, Reason: "not below Avg"}
	case p.Max <= p.Avg:
		return &ParamsError{Field: "Max", Value: p.Max, Reason: "not above Avg"}
	case p.Max > MaxMax:
		return &ParamsError{Field: "Max", Value: p.Max, Reason: "above " + strconv.Itoa(MaxMax)}
	}
	return nil
}

// Chunk is one content-defined chunk of the stream.
type Chunk struct {
	// Offset is the chunk's position in the stream.
	Offset int64
	// Data is the chunk's bytes. It aliases the chunker's buffer and is valid
	// only until the next call to Next.
	Data []byte
}

// Chunker reads a stream and returns it as chunks. It is not safe for
// concurrent use.
type Chunker struct {
	r      io.Reader
	p      Params
	maskS  uint64
	maskL  uint64
	buf    []byte
	start  int
	end    int
	offset int64
	eof    bool
	err    error
}

// New returns a Chunker over r, or a *ParamsError.
func New(r io.Reader, p Params) (*Chunker, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	b := bits.TrailingZeros(uint(p.Avg))
	return &Chunker{
		r:     r,
		p:     p,
		maskS: topBits(b + 2),
		maskL: topBits(b - 2),
		buf:   make([]byte, 2*p.Max),
	}, nil
}

// topBits returns a mask of the n most significant bits. The Gear hash's top
// bits depend on the last 64 bytes, its low bits on only the last few.
func topBits(n int) uint64 {
	return ^uint64(0) << (64 - n)
}

// Next returns the next chunk, or io.EOF after the last. A read error is
// returned as is, and every later call returns it again.
func (c *Chunker) Next() (Chunk, error) {
	if c.err != nil {
		return Chunk{}, c.err
	}
	if err := c.fill(); err != nil {
		c.err = err
		return Chunk{}, err
	}
	if c.start == c.end {
		c.err = io.EOF
		return Chunk{}, io.EOF
	}
	n := c.cut(c.buf[c.start:c.end])
	ch := Chunk{Offset: c.offset, Data: c.buf[c.start : c.start+n : c.start+n]}
	c.start += n
	c.offset += int64(n)
	return ch, nil
}

// fill makes at least Max bytes available, or everything left in the stream.
func (c *Chunker) fill() error {
	if c.eof || c.end-c.start >= c.p.Max {
		return nil
	}
	c.end = copy(c.buf, c.buf[c.start:c.end])
	c.start = 0
	n, err := io.ReadFull(c.r, c.buf[c.end:])
	c.end += n
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		c.eof = true
		return nil
	default:
		return err
	}
}

// cut returns the length of the chunk at the start of data. data holds at
// least Max bytes unless the stream ends within it.
func (c *Chunker) cut(data []byte) int {
	n := len(data)
	if n <= c.p.Min {
		return n
	}
	if n > c.p.Max {
		n = c.p.Max
	}
	normal := min(c.p.Avg, n)
	var fp uint64
	i := c.p.Min
	for ; i < normal; i++ {
		fp = fp<<1 + gear[data[i]]
		if fp&c.maskS == 0 {
			return i + 1
		}
	}
	for ; i < n; i++ {
		fp = fp<<1 + gear[data[i]]
		if fp&c.maskL == 0 {
			return i + 1
		}
	}
	return n
}

// gear is the Gear hash's table of 256 pseudo-random values: the first eight
// bytes of SHA-256 of a fixed label and the byte value. It is part of the
// repository format.
var gear = func() (t [256]uint64) {
	for i := range t {
		sum := sha256.Sum256([]byte("go-objstore backup fastcdc gear " + strconv.Itoa(i)))
		t[i] = binary.BigEndian.Uint64(sum[:8])
	}
	return t
}()
