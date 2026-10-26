package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	enclave "github.com/jeremyhahn/go-enclave"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// opener connects to an objstore server with cfg and returns the store and a
// function that closes the connection.
type opener func(ctx context.Context, cfg *objstore.ClientConfig) (store.PruneStore, func() error, error)

// openSDK connects through the go-objstore Go client SDK.
func openSDK(_ context.Context, cfg *objstore.ClientConfig) (store.PruneStore, func() error, error) {
	c, err := objstore.NewClient(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	st, err := store.NewSDK(c)
	if err != nil {
		return nil, nil, errors.Join(err, c.Close())
	}
	return st, c.Close, nil
}

// maxMessage is the largest gRPC message the CLI sends or receives, and so the
// largest object it moves over gRPC: gRPC's own ceiling, the largest length
// its message framing carries. gRPC's default receive limit of 4 MiB is below
// a default pack. No smaller fixed bound holds every pack the repository
// format allows: a pack carries up to repo.MaxPackSize bytes of sealed chunks
// and a trailer of one entry per chunk, which with the smallest chunks
// outgrows the chunks themselves. The bound caps a message and allocates
// nothing; the client reads every object whole into memory on every protocol,
// and only gRPC caps it.
const maxMessage = math.MaxInt32

var protocols = []objstore.Protocol{
	objstore.ProtocolREST, objstore.ProtocolGRPC, objstore.ProtocolQUIC, objstore.ProtocolMCP, objstore.ProtocolUnix,
}

// connFlags are the settings that connect to an objstore server as one
// identity. The backup identity's set has no name prefix; prune's set is
// named "prune-", so the backup identity's settings never stand in for the
// delete-capable identity prune needs.
type connFlags struct {
	prefix    string
	protocol  string
	address   string
	tls       bool
	caFile    string
	certFile  string
	keyFile   string
	tokenFile string
	tenant    string
	timeout   time.Duration
}

func (c *connFlags) register(fs *flag.FlagSet, prefix, who string) {
	c.prefix = prefix
	fs.StringVar(&c.protocol, prefix+"protocol", string(objstore.ProtocolGRPC),
		"protocol to the "+who+" server: rest, grpc, quic, mcp or unix")
	fs.StringVar(&c.address, prefix+"address", "",
		"address of the "+who+" server (required): host:port, URL or Unix socket path")
	fs.BoolVar(&c.tls, prefix+"tls", false, "connect to the "+who+" server over TLS")
	fs.StringVar(&c.caFile, prefix+"ca-file", "", "CA certificate file that verifies the "+who+" server")
	fs.StringVar(&c.certFile, prefix+"cert-file", "", "client certificate file of the "+who+" identity")
	fs.StringVar(&c.keyFile, prefix+"key-file", "", "client private key file of the "+who+" identity")
	fs.StringVar(&c.tokenFile, prefix+"token-file", "",
		"file holding the "+who+" identity's bearer token: a regular file with no group or other permissions")
	fs.StringVar(&c.tenant, prefix+"tenant", "", "tenant ID of the "+who+" identity")
	fs.DurationVar(&c.timeout, prefix+"timeout", 0, "request timeout to the "+who+" server (0: the SDK's default)")
}

func (c *connFlags) check() error {
	if c.address == "" {
		return fmt.Errorf("--%saddress is required", c.prefix)
	}
	for _, p := range protocols {
		if c.protocol == string(p) {
			return nil
		}
	}
	return fmt.Errorf("--%sprotocol %q is not one of rest, grpc, quic, mcp or unix", c.prefix, c.protocol)
}

// config returns the SDK client settings, reading the token file if one is
// named. The token is never printed or put in an error.
func (c *connFlags) config() (*objstore.ClientConfig, error) {
	cfg := &objstore.ClientConfig{
		Protocol:       objstore.Protocol(c.protocol),
		Address:        c.address,
		UseTLS:         c.tls,
		CAFile:         c.caFile,
		CertFile:       c.certFile,
		KeyFile:        c.keyFile,
		TenantID:       c.tenant,
		RequestTimeout: c.timeout,
		MaxRecvMsgSize: maxMessage,
		MaxSendMsgSize: maxMessage,
	}
	if c.tokenFile != "" {
		buf, err := readSecretFile(c.tokenFile)
		if err != nil {
			return nil, &secretError{what: "token file", source: c.tokenFile, err: err}
		}
		cfg.Token = strings.TrimSpace(string(buf))
		clear(buf)
	}
	return cfg, nil
}

// repoFlags name the repository on the server and where its passphrase
// comes from. The passphrase is never taken from an argument or the
// environment.
type repoFlags struct {
	repo      string
	passFile  string
	passStdin bool
}

func (r *repoFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&r.repo, "repo", "", "repository prefix on the server (default: the server's root)")
	fs.StringVar(&r.passFile, "passphrase-file", "",
		"file holding the repository passphrase: a regular file with no group or other permissions")
	fs.BoolVar(&r.passStdin, "passphrase-stdin", false, "read the repository passphrase from standard input")
}

func (r *repoFlags) check() error {
	switch {
	case r.passFile != "" && r.passStdin:
		return errors.New("--passphrase-file and --passphrase-stdin are exclusive")
	case r.passFile == "" && !r.passStdin:
		return errors.New("a passphrase is required: --passphrase-file or --passphrase-stdin")
	}
	return nil
}

// provider reads the passphrase into locked memory and returns a provider
// that owns it. The buffer the passphrase was read into is cleared before
// provider returns.
func (r *repoFlags) provider(stdin io.Reader, iterations int) (*keyseam.PassphraseProvider, error) {
	var (
		buf []byte
		err error
	)
	if r.passStdin {
		buf, err = readSecret(stdin)
		if err != nil {
			return nil, &secretError{what: "passphrase", source: "from standard input", err: err}
		}
	} else {
		buf, err = readSecretFile(r.passFile)
		if err != nil {
			return nil, &secretError{what: "passphrase file", source: r.passFile, err: err}
		}
	}
	defer clear(buf)
	h, err := enclave.NewMemory(buf)
	if err != nil {
		return nil, fmt.Errorf("lock the passphrase in memory: %w", err)
	}
	p, err := keyseam.NewPassphraseProvider(h, iterations)
	if err != nil {
		return nil, errors.Join(err, h.Destroy())
	}
	return p, nil
}

// newFlags returns a flag set that reports nothing itself: parse does.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	return fs
}

// printFlags prints fs's flags as --name, the form the help text uses.
func printFlags(w io.Writer, fs *flag.FlagSet) {
	fs.VisitAll(func(f *flag.Flag) {
		arg, use := flag.UnquoteUsage(f)
		line := "  --" + f.Name
		if arg != "" {
			line += " " + arg
		}
		fmt.Fprintf(w, "%s\n    \t%s", line, use)
		switch f.DefValue {
		case "", "false", "0", "0s":
		default:
			fmt.Fprintf(w, " (default %s)", f.DefValue)
		}
		fmt.Fprintln(w)
	})
}

// parse parses args for command name, with flags allowed among the
// positional arguments, and checks there are min to max positionals (max -1
// means no limit). It prints the help for -h and a usage error for anything
// else; ok is false when the command must return code.
func (e *env) parse(name, help string, fs *flag.FlagSet, args []string, minArgs, maxArgs int) (pos []string, code int, ok bool) {
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				fmt.Fprint(e.stdout, help)
				fmt.Fprintln(e.stdout, "\nFlags:")
				printFlags(e.stdout, fs)
				return nil, exitOK, false
			}
			return nil, e.usageError(name, err), false
		}
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		if n := len(args) - len(rest); n > 0 && args[n-1] == "--" {
			pos = append(pos, rest...)
			break
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
	switch {
	case len(pos) < minArgs:
		return nil, e.usageError(name, errors.New("missing arguments")), false
	case maxArgs >= 0 && len(pos) > maxArgs:
		return nil, e.usageError(name, fmt.Errorf("unexpected argument %q", pos[maxArgs])), false
	}
	return pos, exitOK, true
}

// days is a duration flag that also takes a whole number of days, "7d".
type days struct{ d *time.Duration }

// maxDays is the largest day count a time.Duration holds.
const maxDays = int64(math.MaxInt64 / (24 * time.Hour))

func (f days) String() string {
	if f.d == nil {
		return ""
	}
	return f.d.String()
}

func (f days) Set(s string) error {
	if n, ok := strings.CutSuffix(s, "d"); ok {
		v, err := strconv.ParseInt(n, 10, 64)
		if err != nil || v < 0 {
			return errors.New("invalid number of days")
		}
		if v > maxDays {
			return fmt.Errorf("invalid number of days: at most %d", maxDays)
		}
		*f.d = time.Duration(v) * 24 * time.Hour
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return errors.New("invalid duration")
	}
	*f.d = v
	return nil
}
