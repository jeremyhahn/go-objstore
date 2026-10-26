package chunker

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
)

var smallParams = Params{Min: 2 << 10, Avg: 8 << 10, Max: 32 << 10}

func randomData(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) //nolint:gosec // test data, not a secret
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// split chunks r and returns copies of the chunks.
func split(t testing.TB, r io.Reader, p Params) [][]byte {
	t.Helper()
	c, err := New(r, p)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var out [][]byte
	var off int64
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if ch.Offset != off {
			t.Fatalf("chunk offset %d, want %d", ch.Offset, off)
		}
		off += int64(len(ch.Data))
		out = append(out, append([]byte(nil), ch.Data...))
	}
}

func checkChunks(t testing.TB, data []byte, chunks [][]byte, p Params) {
	t.Helper()
	if got := bytes.Join(chunks, nil); !bytes.Equal(got, data) {
		t.Fatalf("chunks rejoin to %d bytes, want the %d-byte input", len(got), len(data))
	}
	for i, ch := range chunks {
		last := i == len(chunks)-1
		if len(ch) == 0 || len(ch) > p.Max || (!last && len(ch) < p.Min) {
			t.Fatalf("chunk %d of %d is %d bytes, outside [%d, %d]", i, len(chunks), len(ch), p.Min, p.Max)
		}
	}
}

func TestDeterministicBoundaries(t *testing.T) {
	data := randomData(1, 1<<20)
	whole := split(t, bytes.NewReader(data), smallParams)
	checkChunks(t, data, whole, smallParams)
	if len(whole) < 64 {
		t.Fatalf("%d chunks for 1 MiB at an 8 KiB average", len(whole))
	}
	again := split(t, bytes.NewReader(data), smallParams)
	oneByte := split(t, iotest.OneByteReader(bytes.NewReader(data)), smallParams)
	half := split(t, iotest.HalfReader(bytes.NewReader(data)), smallParams)
	for _, other := range [][][]byte{again, oneByte, half} {
		if len(other) != len(whole) {
			t.Fatalf("%d chunks, want %d", len(other), len(whole))
		}
		for i := range whole {
			if !bytes.Equal(other[i], whole[i]) {
				t.Fatalf("chunk %d differs between runs", i)
			}
		}
	}
}

func TestAverageNearTarget(t *testing.T) {
	data := randomData(2, 4<<20)
	chunks := split(t, bytes.NewReader(data), smallParams)
	avg := len(data) / len(chunks)
	if avg < smallParams.Avg/2 || avg > smallParams.Avg*2 {
		t.Fatalf("average chunk %d bytes, target %d", avg, smallParams.Avg)
	}
}

func TestInsertionChangesOnlyNearbyChunks(t *testing.T) {
	data := randomData(3, 2<<20)
	edited := make([]byte, 0, len(data)+100)
	edited = append(edited, data[:5000]...)
	edited = append(edited, randomData(4, 100)...)
	edited = append(edited, data[5000:]...)

	before := split(t, bytes.NewReader(data), smallParams)
	after := split(t, bytes.NewReader(edited), smallParams)
	checkChunks(t, edited, after, smallParams)

	known := map[[32]byte]bool{}
	for _, ch := range before {
		known[sha256.Sum256(ch)] = true
	}
	changed := 0
	for i, ch := range after {
		if !known[sha256.Sum256(ch)] {
			changed++
			if i > 3 {
				t.Fatalf("chunk %d changed, far after the insertion", i)
			}
		}
	}
	if changed == 0 || changed > 3 {
		t.Fatalf("%d chunks changed after a 100-byte insertion, want 1 to 3", changed)
	}
	if len(after) < len(before)-3 || len(after) > len(before)+3 {
		t.Fatalf("%d chunks after the edit, %d before", len(after), len(before))
	}
}

func TestSmallInputs(t *testing.T) {
	if chunks := split(t, bytes.NewReader(nil), smallParams); len(chunks) != 0 {
		t.Fatalf("empty input gave %d chunks", len(chunks))
	}
	for _, n := range []int{1, 100, smallParams.Min - 1, smallParams.Min, smallParams.Min + 1} {
		data := randomData(uint64(n), n) //nolint:gosec // n is positive
		chunks := split(t, bytes.NewReader(data), smallParams)
		checkChunks(t, data, chunks, smallParams)
		if n <= smallParams.Min && len(chunks) != 1 {
			t.Fatalf("%d-byte input gave %d chunks", n, len(chunks))
		}
	}
}

func TestMaxBoundOnUniformInput(t *testing.T) {
	// Zeros never meet a mask with this table, so every cut is at Max.
	data := make([]byte, 5*smallParams.Max+7)
	chunks := split(t, bytes.NewReader(data), smallParams)
	checkChunks(t, data, chunks, smallParams)
	for _, ch := range chunks[:len(chunks)-1] {
		if len(ch) != smallParams.Max {
			t.Fatalf("chunk of %d bytes in uniform input, want %d", len(ch), smallParams.Max)
		}
	}
}

func TestDefaultParams(t *testing.T) {
	p := DefaultParams()
	if err := p.Validate(); err != nil {
		t.Fatalf("DefaultParams invalid: %v", err)
	}
	data := randomData(5, 3*DefaultMax)
	checkChunks(t, data, split(t, bytes.NewReader(data), p), p)
}

func TestParamsValidate(t *testing.T) {
	tests := []struct {
		p     Params
		field string
	}{
		{Params{Min: 32, Avg: 1024, Max: 4096}, "Min"},
		{Params{Min: 64, Avg: 128, Max: 4096}, "Avg"},
		{Params{Min: 64, Avg: 1000, Max: 4096}, "Avg"},
		{Params{Min: 1024, Avg: 1024, Max: 4096}, "Min"},
		{Params{Min: 64, Avg: 1024, Max: 1024}, "Max"},
		{Params{Min: 64, Avg: 1024, Max: MaxMax + 1}, "Max"},
	}
	for _, tt := range tests {
		_, err := New(bytes.NewReader(nil), tt.p)
		var pe *ParamsError
		if !errors.Is(err, ErrInvalidParams) || !errors.As(err, &pe) || pe.Field != tt.field {
			t.Fatalf("New(%+v) = %v, want a *ParamsError on %s", tt.p, err, tt.field)
		}
		if pe.Error() == "" {
			t.Fatal("empty error text")
		}
	}
}

func TestReadErrorIsSticky(t *testing.T) {
	errRead := errors.New("read failed")
	r := io.MultiReader(bytes.NewReader(randomData(6, 100)), iotest.ErrReader(errRead))
	c, err := New(r, smallParams)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for range 2 {
		if _, err := c.Next(); !errors.Is(err, errRead) {
			t.Fatalf("Next: %v, want the read error", err)
		}
	}
	c, _ = New(bytes.NewReader(nil), smallParams)
	for range 2 {
		if _, err := c.Next(); !errors.Is(err, io.EOF) {
			t.Fatalf("Next on empty input: %v, want io.EOF", err)
		}
	}
}

func FuzzChunker(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte("hello world"), uint8(1))
	f.Add(randomData(7, 3000), uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, sel uint8) {
		p := Params{Min: 64, Avg: 256, Max: 1024}
		if sel%2 == 1 {
			p = Params{Min: 128, Avg: 512, Max: 640}
		}
		whole := split(t, bytes.NewReader(data), p)
		checkChunks(t, data, whole, p)
		oneByte := split(t, iotest.OneByteReader(bytes.NewReader(data)), p)
		if len(oneByte) != len(whole) {
			t.Fatalf("%d chunks one byte at a time, %d whole", len(oneByte), len(whole))
		}
		for i := range whole {
			if !bytes.Equal(oneByte[i], whole[i]) {
				t.Fatalf("chunk %d depends on read sizes", i)
			}
		}
	})
}
