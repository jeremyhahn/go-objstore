package srcsnap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Runner runs a tool. argv[0] is the tool's absolute path; the rest are its
// arguments, passed as they are, never through a shell.
type Runner interface {
	// Run runs argv and returns what it wrote to standard output. A tool
	// that exits non-zero is an error that carries the start of its
	// standard error.
	Run(ctx context.Context, argv []string) ([]byte, error)
}

// maxStderr bounds the standard error kept in an error.
const maxStderr = 1024

// ExecRunner runs tools with os/exec, with LC_ALL=C so their output does
// not depend on the locale.
type ExecRunner struct{}

// Run implements Runner.
func (ExecRunner) Run(ctx context.Context, argv []string) ([]byte, error) {
	if len(argv) == 0 || !filepath.IsAbs(argv[0]) {
		return nil, detail("the tool is not an absolute path")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // an absolute tool path and an argument vector, never a shell
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	cmd.WaitDelay = 10 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > maxStderr {
			msg = msg[:maxStderr] + "..."
		}
		if msg != "" {
			return stdout.Bytes(), fmt.Errorf("%s: %w: %s", filepath.Base(argv[0]), err, msg)
		}
		return stdout.Bytes(), fmt.Errorf("%s: %w", filepath.Base(argv[0]), err)
	}
	return stdout.Bytes(), nil
}

// lookTool finds tool in the system's tool directories. $PATH is not
// searched, so a service's environment cannot redirect a privileged tool.
func lookTool(tool string) (string, error) {
	for _, dir := range toolDirs() {
		p := filepath.Join(dir, tool)
		fi, err := os.Stat(p)
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		if runtime.GOOS != "windows" && fi.Mode().Perm()&0o111 == 0 {
			continue
		}
		return p, nil
	}
	return "", detail("%s is not in %s", tool, strings.Join(toolDirs(), string(os.PathListSeparator)))
}

// toolDirs returns the directories lookTool searches.
func toolDirs() []string {
	if runtime.GOOS == "windows" {
		root := os.Getenv("SystemRoot")
		if root == "" || !filepath.IsAbs(root) {
			root = `C:\Windows`
		}
		return []string{filepath.Join(root, "System32", "WindowsPowerShell", "v1.0")}
	}
	return []string{"/usr/sbin", "/sbin", "/usr/bin", "/bin", "/usr/local/sbin"}
}

// tool resolves a tool once and keeps its path.
func (s *Snapshotter) tool(name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.tools[name]; ok {
		return p, nil
	}
	p, err := s.env.LookPath(name)
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(p) {
		return "", detail("%s resolved to %q, not an absolute path", name, p)
	}
	s.tools[name] = p
	return p, nil
}

// run runs tool with args for step op and wraps a failure in an Error.
func (s *Snapshotter) run(ctx context.Context, m Method, op, volume, name, tool string, args ...string) ([]byte, error) {
	p, err := s.tool(tool)
	if err != nil {
		return nil, &Error{Op: op, Method: m, Volume: volume, Snapshot: name, Err: ErrTool, Cause: err}
	}
	out, err := s.env.Runner.Run(ctx, append([]string{p}, args...))
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return nil, &Error{Op: op, Method: m, Volume: volume, Snapshot: name, Err: ErrCanceled, Cause: errors.Join(cerr, err)}
		}
		return nil, &Error{Op: op, Method: m, Volume: volume, Snapshot: name, Err: ErrCommand, Cause: err}
	}
	return out, nil
}
