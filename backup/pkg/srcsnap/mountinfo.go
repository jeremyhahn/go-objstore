package srcsnap

import (
	"bufio"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

// mountEntry is one line of /proc/self/mountinfo.
type mountEntry struct {
	mountPoint, root, fsType, source string
}

// parseMountinfo parses the mountinfo format of proc(5).
func parseMountinfo(r io.Reader) ([]mountEntry, error) {
	var out []mountEntry
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		sep := -1
		for i := 6; i < len(f); i++ {
			if f[i] == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+2 >= len(f) {
			return nil, detail("malformed mountinfo line")
		}
		out = append(out, mountEntry{
			root:       unescapeMount(f[3]),
			mountPoint: unescapeMount(f[4]),
			fsType:     f[sep+1],
			source:     unescapeMount(f[sep+2]),
		})
	}
	return out, sc.Err()
}

// unescapeMount decodes the octal escapes (\040 for a space) mountinfo
// uses.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// findMount returns the entry whose mount point is the longest prefix of
// path; of entries with the same mount point the last, which is mounted
// over the others, wins.
func findMount(entries []mountEntry, path string) (mountEntry, bool) {
	best, found := mountEntry{}, false
	for _, e := range entries {
		if !within(e.mountPoint, path) {
			continue
		}
		if !found || len(e.mountPoint) >= len(best.mountPoint) {
			best, found = e, true
		}
	}
	return best, found
}

// within reports whether path is dir or inside it.
func within(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
