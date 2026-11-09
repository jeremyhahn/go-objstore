package labstrings

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"
)

// patternsEnv names the rules file. The rules are not in the tree (IS-31): in
// CI the file is the RELEASABILITY_PATTERNS secret. Unset or empty means no
// rules, and the tree test then has nothing to find.
const patternsEnv = "LABSTRINGS_PATTERNS"

// maxPatternsFileSize bounds the rules file.
const maxPatternsFileSize = 1 << 20

// classes are the classes with specific guidance. The rules file states its
// own class set (ruling 411): a line may name any well-formed class, and one
// outside these four gets defaultGuidance.
var classes = []class{labAddress, labHost, ownerPath, ownerAccount}

// classField is a well-formed class field in the rules file.
var classField = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// defaultGuidance is what a finding of a class without specific guidance
// tells the author to write instead.
const defaultGuidance = "remove the value, or replace it with a documentation value: an RFC 5737 address, a name under .example.com, or a repository-relative path"

// classGuidance is what a finding of each class tells the author to write
// instead. It names no fleet value.
var classGuidance = map[class]string{
	labAddress:   "use an RFC 5737 documentation address: 192.0.2.0/24, 198.51.100.0/24 or 203.0.113.0/24",
	labHost:      "name the role, not the machine: agent-01, build-01.example, server.example.com",
	ownerPath:    "use a repository-relative path, or /var/lib, /srv, /home/agent",
	ownerAccount: "use a role (operator, agent) or the module path; the module owner in the module path is public and is not a violation",
}

// patterns is the rule set, loaded once from the rules file. patternsErr is
// the loader's error, which the tree test reports; it names the file and line
// only.
var patterns, patternsErr = loadPatterns(os.Getenv(patternsEnv))

// patternsFileError is a bad line in the rules file. It never carries the
// line's text.
type patternsFileError struct {
	Path    string
	Line    int
	Problem string
}

func (e *patternsFileError) Error() string {
	return fmt.Sprintf("labstrings: patterns file %s: line %d: %s", e.Path, e.Line, e.Problem)
}

// loadPatterns reads the patterns file at path. An empty path means no file:
// the scanner then has no lab patterns and reports nothing.
func loadPatterns(path string) ([]pattern, error) {
	if path == "" {
		return nil, nil
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return nil, fmt.Errorf("labstrings: patterns file %s: not an absolute path", clean)
	}
	info, err := os.Lstat(clean) // #nosec G703 -- a stat of the cleaned absolute path, to validate it
	if err != nil {
		return nil, fmt.Errorf("labstrings: patterns file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("labstrings: patterns file %s: not a regular file", clean)
	}
	if info.Size() > maxPatternsFileSize {
		return nil, fmt.Errorf("labstrings: patterns file %s: larger than %d bytes", clean, maxPatternsFileSize)
	}
	data, err := os.ReadFile(clean) // #nosec G304 G703 -- the operator's own patterns file, validated above
	if err != nil {
		return nil, fmt.Errorf("labstrings: patterns file: %w", err)
	}
	return parsePatterns(path, data)
}

// parsePatterns parses one rule per line, class<TAB>regexp. Blank lines and
// lines starting with # are skipped.
func parsePatterns(path string, data []byte) ([]pattern, error) {
	var ps []pattern
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		c, expr, ok := strings.Cut(line, "\t")
		if !ok {
			return nil, &patternsFileError{Path: path, Line: i + 1, Problem: "want class<TAB>regexp"}
		}
		if !classField.MatchString(c) {
			return nil, &patternsFileError{Path: path, Line: i + 1, Problem: "malformed class field"}
		}
		if expr == "" {
			return nil, &patternsFileError{Path: path, Line: i + 1, Problem: "empty regexp"}
		}
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, &patternsFileError{Path: path, Line: i + 1, Problem: "regexp does not compile"}
		}
		ps = append(ps, newPattern(class(c), re))
	}
	return ps, nil
}

func newPattern(c class, re *regexp.Regexp) pattern {
	guidance, ok := classGuidance[c]
	if !ok {
		guidance = defaultGuidance
	}
	return pattern{Class: c, Expr: re, Needle: needleOf(re), Guidance: guidance}
}

// needleOf returns the longest literal that every match of re must contain,
// lowercased for the case-folded sieve, or "" when re has none.
func needleOf(re *regexp.Regexp) string {
	tree, err := syntax.Parse(re.String(), syntax.Perl)
	if err != nil {
		return ""
	}
	return strings.ToLower(mandatoryLiteral(tree.Simplify()))
}

func mandatoryLiteral(re *syntax.Regexp) string {
	switch re.Op {
	case syntax.OpLiteral:
		return string(re.Rune)
	case syntax.OpCapture, syntax.OpPlus:
		return mandatoryLiteral(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min >= 1 {
			return mandatoryLiteral(re.Sub[0])
		}
	case syntax.OpConcat:
		best := ""
		for _, sub := range re.Sub {
			if lit := mandatoryLiteral(sub); len(lit) > len(best) {
				best = lit
			}
		}
		return best
	}
	return ""
}

// setPatterns replaces the rule set and returns a function that restores the
// previous one. It is the tests' hook: the real patterns are not in the tree.
func setPatterns(ps []pattern) (restore func()) {
	oldPatterns, oldErr := patterns, patternsErr
	patterns, patternsErr = ps, nil
	return func() {
		patterns, patternsErr = oldPatterns, oldErr
	}
}

func writePatternsFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "patterns.tsv")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadPatterns_UnsetMeansNoRules(t *testing.T) {
	ps, err := loadPatterns("")
	if err != nil || ps != nil {
		t.Fatalf("got %d rules, err %v; want none", len(ps), err)
	}
}

func TestLoadPatterns_GoodFile(t *testing.T) {
	p := writePatternsFile(t, "# neutral rules\n\n"+
		"lab-address\t\\b198\\.51\\.100\\.\\d{1,3}\\b\r\n"+
		"lab-host\t\\bbuild-\\d{2}\\.lab\\.example\\b\n"+
		"owner-path\t/home/example-user\\b\n"+
		"owner-account\t\\bExample-User\\b\n")
	ps, err := loadPatterns(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		class  class
		needle string
	}{
		{labAddress, "198.51.100."},
		{labHost, ".lab.example"},
		{ownerPath, "/home/example-user"},
		{ownerAccount, "example-user"},
	}
	if len(ps) != len(want) {
		t.Fatalf("%d rules, want %d", len(ps), len(want))
	}
	for i, w := range want {
		if ps[i].Class != w.class || ps[i].Needle != w.needle || ps[i].Guidance != classGuidance[w.class] {
			t.Errorf("rule %d (%s): want class %s, its derived needle and guidance", i, ps[i].Class, w.class)
		}
	}
}

func TestLoadPatterns_BadLines(t *testing.T) {
	const secret = "198.51.100.77"
	for name, c := range map[string]struct {
		content string
		problem string
	}{
		"no tab":           {"lab-address " + secret + "\n", "want class<TAB>regexp"},
		"malformed class":  {"Lab_Printer\t" + secret + "\n", "malformed class field"},
		"empty regexp":     {"lab-address\t\n", "empty regexp"},
		"does not compile": {"lab-address\t(" + secret + "\n", "regexp does not compile"},
	} {
		t.Run(name, func(t *testing.T) {
			p := writePatternsFile(t, "# header\nlab-host\tci-\\d+\n"+c.content)
			_, err := loadPatterns(p)
			var fe *patternsFileError
			if !errors.As(err, &fe) {
				t.Fatalf("want a patternsFileError, got %v", err)
			}
			if fe.Line != 3 || fe.Problem != c.problem || fe.Path != p {
				t.Errorf("got line %d, problem %q; want line 3, %q", fe.Line, fe.Problem, c.problem)
			}
			msg := err.Error()
			if strings.Contains(msg, secret) || strings.Contains(msg, "Lab_Printer") || strings.Contains(msg, "("+secret) {
				t.Error("the error carries the line's text")
			}
			if !strings.Contains(msg, p) || !strings.Contains(msg, "line 3") {
				t.Errorf("the error does not name the file and line: %s", msg)
			}
		})
	}
}

func TestLoadPatterns_FileStatesItsClasses(t *testing.T) {
	p := writePatternsFile(t, "lab-printer\t\\bprinter-\\d{2}\\.lab\\.example\\b\n")
	ps, err := loadPatterns(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 1 || ps[0].Class != "lab-printer" || ps[0].Guidance != defaultGuidance {
		t.Fatalf("got %d rules; want one lab-printer rule with the default guidance", len(ps))
	}
	defer setPatterns(ps)()
	got := scan("docs/x.md", []byte("see printer-07.lab.example\n"))
	if len(got) != 1 || got[0].Class != "lab-printer" || got[0].Line != 1 {
		t.Fatalf("got %v; want one lab-printer finding on line 1", got)
	}
}

func TestClassGuidance_EveryDeclaredClass(t *testing.T) {
	for _, c := range classes {
		if g := classGuidance[c]; g == "" || g == defaultGuidance {
			t.Errorf("class %s has no specific guidance", c)
		}
	}
}

func TestLoadPatterns_RelativePath(t *testing.T) {
	if _, err := loadPatterns(filepath.Join("testdata", "patterns.tsv")); err == nil || !strings.Contains(err.Error(), "not an absolute path") {
		t.Fatalf("want a not-absolute error, got %v", err)
	}
}

func TestLoadPatterns_Symlink(t *testing.T) {
	target := writePatternsFile(t, "lab-host\tci-\\d+\n")
	link := filepath.Join(t.TempDir(), "link.tsv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("no symlinks here: %v", err)
	}
	if _, err := loadPatterns(link); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("want a not-regular error, got %v", err)
	}
}

func TestLoadPatterns_Directory(t *testing.T) {
	if _, err := loadPatterns(t.TempDir()); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("want a not-regular error, got %v", err)
	}
}

func TestLoadPatterns_Missing(t *testing.T) {
	if _, err := loadPatterns(filepath.Join(t.TempDir(), "absent.tsv")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("want not-exist, got %v", err)
	}
}

func TestLoadPatterns_Oversize(t *testing.T) {
	p := writePatternsFile(t, "# "+strings.Repeat("x", maxPatternsFileSize)+"\n")
	if _, err := loadPatterns(p); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("want an oversize error, got %v", err)
	}
}

func TestSetPatterns_Restores(t *testing.T) {
	before := len(patterns)
	restore := setPatterns(testPatterns())
	if len(patterns) != len(testPatterns()) || patternsErr != nil {
		t.Fatal("setPatterns did not install the rules")
	}
	restore()
	if len(patterns) != before {
		t.Fatal("restore did not put the previous rules back")
	}
}
