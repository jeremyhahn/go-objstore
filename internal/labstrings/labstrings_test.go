// Package labstrings is the public-releasability gate (#51): a test-only
// package that fails when a published file names the private development fleet
// or its owner. See docs/release/public-releasability.md.
//
// The package has no non-test code and no exported API. The rules are not in
// the tree (IS-31): they load at test time from the file the
// LABSTRINGS_PATTERNS environment variable names (patterns_test.go); in CI
// that file is the RELEASABILITY_PATTERNS secret. With the variable unset there
// are no rules and the tree test finds nothing. Nothing prints a rule or a
// match: a finding names its file, line, class and guidance only. The waiver,
// the allow-list and the published-file manifest live here.
package labstrings

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// class names one kind of fleet value.
type class string

const (
	labAddress   class = "lab-address"
	labHost      class = "lab-host"
	ownerPath    class = "owner-path"
	ownerAccount class = "owner-account"
)

// pattern is one fleet value the gate looks for. Needle is a lowercase literal
// every match of Expr must contain, derived from Expr by needleOf: the scan
// sieves on needles first and only runs the regexps on lines that survive. An
// empty needle never skips a line.
type pattern struct {
	Class    class
	Expr     *regexp.Regexp
	Needle   string
	Guidance string
}

// finding is one offending line. It carries no match text, so no finding can
// print a fleet value.
type finding struct {
	Path     string
	Line     int
	Class    class
	Guidance string
}

func (f finding) String() string {
	return fmt.Sprintf("%s:%d: %s: %s", f.Path, f.Line, f.Class, f.Guidance)
}

// waiverRe is the inline waiver: "lab-string-ok:" followed by a reason. A
// waiver with no reason is not a waiver.
var waiverRe = regexp.MustCompile(`lab-string-ok:\s*\S`)

// scan reports every unwaived pattern match in content. A waiver covers its own
// line and the line below it.
func scan(rel string, content []byte) []finding {
	low := bytes.ToLower(content)
	hit := false
	for _, p := range patterns {
		if bytes.Contains(low, []byte(p.Needle)) {
			hit = true
			break
		}
	}
	if !hit {
		return nil
	}
	var out []finding
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if waiverRe.MatchString(line) || (i > 0 && waiverRe.MatchString(lines[i-1])) {
			continue
		}
		lowLine := strings.ToLower(line)
		for _, p := range patterns {
			if !strings.Contains(lowLine, p.Needle) {
				continue
			}
			if p.Expr.MatchString(line) {
				out = append(out, finding{rel, i + 1, p.Class, p.Guidance})
			}
		}
	}
	return out
}

// allowEntry accepts a class of values under a path prefix. Reason is mandatory.
type allowEntry struct {
	Prefix  string
	Classes []class
	Reason  string
}

// defaultAllowList is for a tree, or a file that cannot carry a comment. Only
// the gate's own files may appear here. It is empty today: the gate's own lines
// carry inline waivers and its fixtures are assembled at run time.
var defaultAllowList = []allowEntry{}

func (e allowEntry) valid() error {
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Errorf("allow-list entry %q has no reason", e.Prefix)
	}
	if !strings.HasPrefix(e.Prefix, "internal/labstrings/") {
		return fmt.Errorf("allow-list entry %q is outside the gate's own files", e.Prefix)
	}
	if len(e.Classes) == 0 {
		return fmt.Errorf("allow-list entry %q accepts no class", e.Prefix)
	}
	return nil
}

func allowed(list []allowEntry, f finding) bool {
	for _, e := range list {
		if !strings.HasPrefix(f.Path, e.Prefix) {
			continue
		}
		for _, c := range e.Classes {
			if c == f.Class {
				return true
			}
		}
	}
	return false
}

// isDevelopmentTooling reports whether rel is an agent-workflow file that is
// never published.
func isDevelopmentTooling(rel string) bool {
	base := path.Base(rel)
	if base == "CLAUDE.md" || base == "AGENTS.md" {
		return true
	}
	return strings.HasPrefix(rel, ".claude/") || strings.HasPrefix(rel, ".serena/")
}

func repoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func git(t testing.TB, dir string, stdin []byte, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("git %v unavailable: %v", args, err)
	}
	return out
}

// publishedFiles is the manifest: the index minus the export-ignored paths.
func publishedFiles(t testing.TB, root string) []string {
	t.Helper()
	listed := bytes.Split(bytes.TrimRight(git(t, root, nil, "ls-files", "-z"), "\x00"), []byte{0})
	var stdin []byte
	var names []string
	for _, n := range listed {
		if len(n) == 0 {
			continue
		}
		names = append(names, string(n))
		stdin = append(append(stdin, n...), 0)
	}
	attrs := bytes.Split(bytes.TrimRight(git(t, root, stdin, "check-attr", "-z", "--stdin", "export-ignore"), "\x00"), []byte{0})
	ignored := map[string]bool{}
	for i := 0; i+2 < len(attrs); i += 3 {
		if string(attrs[i+2]) == "set" {
			ignored[string(attrs[i])] = true
		}
	}
	var out []string
	for _, n := range names {
		if !ignored[n] {
			out = append(out, n)
		}
	}
	return out
}

func TestTheTrackedTreeIsPubliclyReleasable(t *testing.T) {
	if patternsErr != nil {
		t.Fatal(patternsErr) // names the file and line only
	}
	root := repoRoot(t)
	for _, e := range defaultAllowList {
		if err := e.valid(); err != nil {
			t.Fatal(err)
		}
	}
	var bad []string
	for _, rel := range publishedFiles(t, root) {
		content, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			continue // listed in the index, removed from the working tree
		}
		probe := content
		if len(probe) > 8000 {
			probe = probe[:8000]
		}
		if bytes.IndexByte(probe, 0) >= 0 {
			continue // binary
		}
		for _, f := range scan(rel, content) {
			if !allowed(defaultAllowList, f) {
				bad = append(bad, f.String())
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("%d published line(s) name the private fleet or its owner:\n%s", len(bad), strings.Join(bad, "\n"))
	}
}

// testPatterns are neutral rules, one or two per class, for the scan tests: an
// RFC 5737 address block, host names under .example and an example account.
// They stand in for the real rules, which are not in the tree.
func testPatterns() []pattern {
	return []pattern{
		newPattern(labAddress, regexp.MustCompile(`\b198\.51\.100\.\d{1,3}\b`)),
		newPattern(labHost, regexp.MustCompile(`\bbuild-\d{2}\.lab\.example\b`)),
		newPattern(labHost, regexp.MustCompile(`\bci-\d{2}\b`)),
		newPattern(ownerPath, regexp.MustCompile(`/home/example-user\b`)),
		newPattern(ownerAccount, regexp.MustCompile(`\bexample-user\b`)),
	}
}

// useTestPatterns installs testPatterns for the rest of t.
func useTestPatterns(t *testing.T) {
	t.Helper()
	t.Cleanup(setPatterns(testPatterns()))
}

const (
	fxAddr    = "198.51.100.5"
	fxHost    = "build-07.lab.example"
	fxPath    = "/home/example-user/src"
	fxAccount = "example-user"
)

func TestPatterns_Match(t *testing.T) {
	useTestPatterns(t)
	cases := []struct {
		line  string
		class class
	}{
		{"dial " + fxAddr + ":443", labAddress},
		{"ssh " + fxHost, labHost},
		{"ssh ci-03", labHost},
		{"cd " + fxPath, ownerPath},
		{"owner " + fxAccount + " ran it", ownerAccount},
	}
	for _, c := range cases {
		got := scan("x.md", []byte(c.line))
		found := false
		for _, f := range got {
			found = found || f.Class == c.class
		}
		if !found {
			t.Errorf("%q: class %s not reported (got %v)", c.line, c.class, got)
		}
	}
}

func TestPatterns_NotViolations(t *testing.T) {
	useTestPatterns(t)
	for _, line := range []string{
		"addr 198.51.10.1 is opaque",
		"addr 198.51.1000.1",
		"import github.com/anexample-user/go-objstore",
		"https://github.com/example-users/go-enclave",
		"192.0.2.7 and 203.0.113.4",
		"a ci-pipeline, ci-123 and build-7.lab.example",
	} {
		if got := scan("x.md", []byte(line)); len(got) != 0 {
			t.Errorf("%q: unexpected %v", line, got)
		}
	}
}

func TestPatterns_EveryNeedleIsImpliedByItsExpr(t *testing.T) {
	ps := testPatterns()
	samples := []string{fxAddr, fxHost, "ci-03", fxPath, fxAccount}
	if len(samples) != len(ps) {
		t.Fatalf("%d samples for %d rules", len(samples), len(ps))
	}
	for i, p := range ps {
		t.Run(fmt.Sprintf("rule %d (%s)", i, p.Class), func(t *testing.T) {
			if p.Needle == "" {
				t.Error("no needle derived")
			}
			if p.Needle != strings.ToLower(p.Needle) {
				t.Error("needle is not lowercase")
			}
			if m := p.Expr.FindString(samples[i]); m == "" || !strings.Contains(strings.ToLower(m), p.Needle) {
				t.Error("needle is not implied by a match of the rule")
			}
			if strings.TrimSpace(p.Guidance) == "" {
				t.Error("no guidance")
			}
		})
	}
}

func TestPatterns_EmptyNeedleNeverSkips(t *testing.T) {
	p := newPattern(labAddress, regexp.MustCompile(`\b198\.51\.100\.\d+|\b203\.0\.113\.\d+`))
	if p.Needle != "" {
		t.Fatalf("an alternation has no mandatory literal, got a needle of %d bytes", len(p.Needle))
	}
	t.Cleanup(setPatterns([]pattern{p}))
	if got := scan("x", []byte("x 203.0.113.9")); len(got) != 1 {
		t.Errorf("%d findings, want 1", len(got))
	}
}

func TestFinding_StringOmitsTheMatch(t *testing.T) {
	useTestPatterns(t)
	got := scan("docs/x.md", []byte("ssh "+fxHost))
	if len(got) != 1 {
		t.Fatalf("%d findings, want 1", len(got))
	}
	s := got[0].String()
	if strings.Contains(s, fxHost) || strings.Contains(s, "build-07") {
		t.Errorf("finding prints the matched text: %s", s)
	}
	if want := "docs/x.md:1: lab-host: "; !strings.HasPrefix(s, want) {
		t.Errorf("finding %q does not start with %q", s, want)
	}
}

func TestWaiver(t *testing.T) {
	useTestPatterns(t)
	cases := []struct {
		name    string
		content string
		want    int
	}{
		{"same line with reason", "x " + fxAddr + " // lab-string-ok: the value is the subject", 0},
		{"line above with reason", "# lab-string-ok: the value is the subject\nx " + fxAddr, 0},
		{"no reason", "x " + fxAddr + " // lab-string-ok:", 1},
		{"blank reason", "x " + fxAddr + " // lab-string-ok:   ", 1},
		{"two lines above", "# lab-string-ok: reason\n\nx " + fxAddr, 1},
		{"no waiver", "x " + fxAddr, 1},
	}
	for _, c := range cases {
		if got := scan("x", []byte(c.content)); len(got) != c.want {
			t.Errorf("%s: %d findings, want %d: %v", c.name, len(got), c.want, got)
		}
	}
}

func TestAllowList(t *testing.T) {
	good := allowEntry{"internal/labstrings/testdata/", []class{labAddress}, "fixture"}
	if err := good.valid(); err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]allowEntry{
		"no reason":    {"internal/labstrings/x", []class{labAddress}, " "},
		"outside gate": {"docs/", []class{labAddress}, "because"},
		"no classes":   {"internal/labstrings/x", nil, "because"},
	} {
		if e.valid() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	f := finding{Path: "internal/labstrings/testdata/a.txt", Class: labAddress}
	if !allowed([]allowEntry{good}, f) {
		t.Error("covered finding not allowed")
	}
	f.Class = ownerPath
	if allowed([]allowEntry{good}, f) {
		t.Error("uncovered class allowed")
	}
	for _, e := range defaultAllowList {
		if err := e.valid(); err != nil {
			t.Error(err)
		}
	}
}

func TestPublishedFiles_ExcludesExportIgnored(t *testing.T) {
	root := repoRoot(t)
	files := publishedFiles(t, root)
	if len(files) == 0 {
		t.Fatal("empty manifest")
	}
	for _, f := range files {
		if isDevelopmentTooling(f) {
			t.Errorf("manifest holds development tooling %s", f)
		}
	}
	found := false
	for _, f := range files {
		found = found || f == "go.mod"
	}
	if !found {
		t.Error("manifest lacks go.mod")
	}
}

func toolingPaths() []string {
	return []string{"CLAUDE.md", "AGENTS.md", "sub/dir/AGENTS.md", "sub/CLAUDE.md", ".claude/agents/x.md", ".serena/project.yml"}
}

func TestGitattributesExportIgnoresDevelopmentTooling(t *testing.T) {
	root := repoRoot(t)
	dir := t.TempDir()
	attrs, err := os.ReadFile(filepath.Join(root, ".gitattributes"))
	if err != nil {
		t.Fatal(err)
	}
	git(t, dir, nil, "init", "-q")
	if err := os.WriteFile(filepath.Join(dir, ".gitattributes"), attrs, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, p := range append(toolingPaths(), "README.md", "pkg/a.go") {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, dir, nil, "add", "-A")
	git(t, dir, nil, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "x")
	tr := tar.NewReader(bytes.NewReader(git(t, dir, nil, "archive", "HEAD")))
	got := map[string]bool{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got[strings.TrimSuffix(h.Name, "/")] = true
	}
	for _, p := range toolingPaths() {
		if got[p] {
			t.Errorf("git archive ships %s", p)
		}
	}
	for _, p := range []string{"README.md", "pkg/a.go"} {
		if !got[p] {
			t.Errorf("git archive dropped %s", p)
		}
	}
}

func TestTheDockerBuildContextExcludesDevelopmentTooling(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		have[strings.TrimSpace(l)] = true
	}
	for _, want := range []string{"**/CLAUDE.md", "**/AGENTS.md", ".claude/", ".serena/"} {
		if !have[want] {
			t.Errorf(".dockerignore lacks %s", want)
		}
	}
}
