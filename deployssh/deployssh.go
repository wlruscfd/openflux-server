// Package deployssh runs deploy/install.sh on a remote VPS over SSH, non-interactively.
package deployssh

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

const (
	connectTimeout     = 15 * time.Second
	tcpKeepAlivePeriod = 15 * time.Second
	execTimeout        = 90 * time.Second
	pollChunkBytes     = 262144

	remoteDir = "/var/lib/openflux-deploy"

	defaultDeployScriptURL = "https://raw.githubusercontent.com/wlruscfd/openflux-deploy/main/install.sh"
)

var (
	followTimeout   = 75 * time.Minute
	pollInterval    = 2 * time.Second
	reconnectDelay  = 4 * time.Second
	reconnectWindow = 10 * time.Minute
)

type SSHTarget struct {
	Host     string
	Port     int
	Username string

	AuthMethod    string
	Password      string
	PrivateKeyPEM string
	Passphrase    string

	KnownHostKeyFingerprint string
}

type DeployOptions struct {
	DeployScriptURL string

	RepoURL string
	GitRef  string

	TLSMode  string
	Domain   string
	Email    string
	ServerIP string

	AdminToken string
	DBPassword string

	RegisterNode bool
	NodeName     string
	NodeMaxKeys  int
	RunNodeHere  bool

	AttachOnly bool
}

type Callback interface {
	OnLog(line string)
	OnHostKeyFingerprint(fingerprint string)
	OnDeployResult(panelURL, adminToken, nodeToken string)
}

var errHostKeyChanged = errors.New("host key changed")

type remote struct {
	target      SSHTarget
	cb          Callback
	client      *ssh.Client
	fingerprint string
	reported    bool
	sudo        string
	stdin       string
}

func Deploy(target SSHTarget, opts DeployOptions, cb Callback) error {
	if cb == nil {
		cb = noopCallback{}
	}
	if strings.TrimSpace(target.Host) == "" {
		return errors.New("the server address is empty")
	}
	if target.Port == 0 {
		target.Port = 22
	}
	if target.Username == "" {
		target.Username = "root"
	}

	r := &remote{target: target, cb: cb}
	if err := r.connect(); err != nil {
		return err
	}
	defer r.close()

	if err := r.detectPrivileges(); err != nil {
		return err
	}
	if !opts.AttachOnly {
		if err := r.start(opts); err != nil {
			return err
		}
	}
	return r.follow(opts.AttachOnly)
}

func (r *remote) connect() error {
	authMethod, err := buildAuthMethod(r.target)
	if err != nil {
		return fmt.Errorf("build ssh auth: %w", err)
	}

	config := &ssh.ClientConfig{
		User:    r.target.Username,
		Auth:    []ssh.AuthMethod{authMethod},
		Timeout: connectTimeout,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			fp := ssh.FingerprintSHA256(key)
			expected := r.target.KnownHostKeyFingerprint
			if r.fingerprint != "" {
				expected = r.fingerprint
			}
			if expected != "" && expected != fp {
				return fmt.Errorf(
					"%w for %s: expected %s, got %s - remove and re-add this server if this is expected (rebuild, reinstall), otherwise treat this as a possible attack and do not proceed",
					errHostKeyChanged, hostname, expected, fp,
				)
			}
			r.fingerprint = fp
			return nil
		},
	}

	addr := net.JoinHostPort(r.target.Host, strconv.Itoa(r.target.Port))
	tcpConn, err := (&net.Dialer{Timeout: connectTimeout, KeepAlive: tcpKeepAlivePeriod}).Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	sshConn, chans, reqs, err := ssh.NewClientConn(tcpConn, addr, config)
	if err != nil {
		tcpConn.Close()
		return fmt.Errorf("ssh handshake %s: %w", addr, err)
	}
	r.client = ssh.NewClient(sshConn, chans, reqs)

	if !r.reported {
		r.reported = true
		r.cb.OnHostKeyFingerprint(r.fingerprint)
	}
	return nil
}

func (r *remote) close() {
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
}

func (r *remote) exec(command string) (string, string, error) {
	if r.client == nil {
		return "", "", errors.New("not connected")
	}
	session, err := r.client.NewSession()
	if err != nil {
		return "", "", err
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if r.stdin != "" {
		session.Stdin = strings.NewReader(r.stdin)
	}

	done := make(chan error, 1)
	go func() { done <- session.Run(command) }()

	select {
	case err := <-done:
		return stdout.String(), stderr.String(), err
	case <-time.After(execTimeout):
		r.close()
		return stdout.String(), stderr.String(), errors.New("the server did not answer in time")
	}
}

func isCommandFailure(err error) bool {
	var exitErr *ssh.ExitError
	return errors.As(err, &exitErr)
}

func (r *remote) detectPrivileges() error {
	stdout, stderr, err := r.exec("id -u")
	if err != nil {
		return fmt.Errorf("check the ssh user: %w%s", err, detail(stderr))
	}
	if strings.TrimSpace(lastLine(stdout)) == "0" {
		return nil
	}

	r.sudo = "sudo -S -p '' "
	r.stdin = r.target.Password + "\n"
	r.cb.OnLog(fmt.Sprintf("connected as %q, not root: using sudo", r.target.Username))
	_, stderr, err = r.exec(r.sudo + "true")
	if err != nil {
		if isCommandFailure(err) {
			return fmt.Errorf("the user %q is not root and cannot use sudo without a prompt that works here: connect as root, or add the user to sudoers%s", r.target.Username, detail(stderr))
		}
		return fmt.Errorf("check sudo: %w", err)
	}
	return nil
}

func (r *remote) start(opts DeployOptions) error {
	script := buildStartScript(opts)
	stdout, stderr, err := r.exec(r.sudo + "bash -c " + shellQuote(script))
	for _, line := range strings.Split(strings.TrimRight(stdout+stderr, "\n"), "\n") {
		if strings.TrimSpace(line) != "" {
			r.cb.OnLog(line)
		}
	}
	if err != nil {
		if isCommandFailure(err) {
			return errors.New("could not start the deploy on the server (details above)")
		}
		return fmt.Errorf("start the deploy: %w", err)
	}
	return nil
}

type pollStatus struct {
	kind string
	code int
}

func (r *remote) poll(offset int64) (pollStatus, []byte, error) {
	stdout, stderr, err := r.exec(r.sudo + "bash -c " + shellQuote(buildPollScript(offset)))
	if err != nil {
		return pollStatus{}, nil, fmt.Errorf("%w%s", err, detail(stderr))
	}
	idx := strings.Index(stdout, "STATUS ")
	if idx < 0 {
		return pollStatus{}, nil, errors.New("unexpected answer from the server")
	}
	rest := stdout[idx:]
	newline := strings.IndexByte(rest, '\n')
	header, body := rest, ""
	if newline >= 0 {
		header, body = rest[:newline], rest[newline+1:]
	}
	fields := strings.Fields(header)
	status := pollStatus{kind: fields[1]}
	if len(fields) > 2 {
		status.code, _ = strconv.Atoi(fields[2])
	}
	return status, []byte(body), nil
}

func (r *remote) follow(attachOnly bool) error {
	deadline := time.Now().Add(followTimeout)
	var offset int64
	var pending []byte

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("the install is still running after %s; it keeps going on the server - open the deploy again later to see how it ended", followTimeout)
		}

		status, data, err := r.poll(offset)
		if err != nil {
			if reconnectErr := r.reconnect(err); reconnectErr != nil {
				return reconnectErr
			}
			continue
		}

		offset += int64(len(data))
		pending = r.emitLines(append(pending, data...))

		switch status.kind {
		case "done", "dead":
			if len(data) > 0 {
				continue
			}
			r.flush(pending)
			if status.kind == "dead" {
				return errors.New("install.sh was interrupted on the server (killed, or the server rebooted) - it is safe to deploy again")
			}
			if status.code != 0 {
				return fmt.Errorf("install.sh failed with exit code %d", status.code)
			}
			return nil
		case "none":
			if attachOnly {
				return errors.New("no deploy is running on the server")
			}
			return errors.New("the deploy did not start on the server")
		}

		if len(data) < pollChunkBytes {
			time.Sleep(pollInterval)
		}
	}
}

func (r *remote) reconnect(cause error) error {
	r.cb.OnLog(fmt.Sprintf("connection lost (%v), reconnecting...", cause))
	r.close()
	until := time.Now().Add(reconnectWindow)
	for {
		time.Sleep(reconnectDelay)
		err := r.connect()
		if err == nil {
			r.cb.OnLog("reconnected")
			return nil
		}
		if errors.Is(err, errHostKeyChanged) || strings.Contains(err.Error(), "unable to authenticate") {
			return err
		}
		if time.Now().After(until) {
			return fmt.Errorf("lost the connection and could not restore it for %s (the install keeps going on the server): %w", reconnectWindow, err)
		}
	}
}

func (r *remote) emitLines(buf []byte) []byte {
	for {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			return buf
		}
		r.emitLine(string(buf[:i]))
		buf = buf[i+1:]
	}
}

func (r *remote) flush(buf []byte) {
	if len(bytes.TrimSpace(buf)) > 0 {
		r.emitLine(string(buf))
	}
}

func (r *remote) emitLine(line string) {
	line = strings.TrimRight(line, "\r")
	if i := strings.LastIndexByte(line, '\r'); i >= 0 {
		line = line[i+1:]
	}
	if fields, ok := strings.CutPrefix(line, resultLinePrefix); ok {
		reportDeployResult(fields, r.cb)
		return
	}
	r.cb.OnLog(line)
}

func detail(stderr string) string {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return ""
	}
	return ": " + stderr
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func buildAuthMethod(target SSHTarget) (ssh.AuthMethod, error) {
	switch target.AuthMethod {
	case "key":
		var signer ssh.Signer
		var err error
		if target.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(target.PrivateKeyPEM), []byte(target.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(target.PrivateKeyPEM))
		}
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		return ssh.PublicKeys(signer), nil
	case "password", "":
		return ssh.Password(target.Password), nil
	default:
		return nil, fmt.Errorf("unknown auth method %q", target.AuthMethod)
	}
}

const resultLinePrefix = "OPENFLUX_DEPLOY_RESULT "

func reportDeployResult(fields string, cb Callback) {
	values := make(map[string]string)
	for _, tok := range strings.Fields(fields) {
		key, value, ok := strings.Cut(tok, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	cb.OnDeployResult(values["panel_url"], values["admin_token"], values["node_token"])
}

func buildEnv(opts DeployOptions) map[string]string {
	maxKeys := ""
	if opts.NodeMaxKeys > 0 {
		maxKeys = strconv.Itoa(opts.NodeMaxKeys)
	}
	return map[string]string{
		"REPO_URL":      opts.RepoURL,
		"GIT_REF":       opts.GitRef,
		"TLS_MODE":      opts.TLSMode,
		"DOMAIN":        opts.Domain,
		"LE_EMAIL":      opts.Email,
		"SERVER_IP":     opts.ServerIP,
		"ADMIN_TOKEN":   opts.AdminToken,
		"DB_PASSWORD":   opts.DBPassword,
		"REGISTER_NODE": boolToYN(opts.RegisterNode),
		"NODE_NAME":     opts.NodeName,
		"NODE_MAX_KEYS": maxKeys,
		"RUN_NODE_HERE": boolToYN(opts.RunNodeHere),
		"WEB_PANEL":     webPanelFor(opts.TLSMode),
	}
}

func webPanelFor(tlsMode string) string {
	if tlsMode == "http" {
		return "n"
	}
	return "y"
}

func buildEnvFile(opts DeployOptions) string {
	env := buildEnv(opts)
	keys := make([]string, 0, len(env))
	for key, value := range env {
		if value != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "export %s=%s\n", key, shellQuote(env[key]))
	}
	return b.String()
}

func buildStartScript(opts DeployOptions) string {
	scriptURL := opts.DeployScriptURL
	if scriptURL == "" {
		scriptURL = defaultDeployScriptURL
	}

	var b strings.Builder
	fmt.Fprintf(&b, "set -e\nD=%s\numask 077\nmkdir -p \"$D\"\n", remoteDir)
	b.WriteString("if [ -s \"$D/pid\" ] && [ ! -f \"$D/rc\" ] && kill -0 \"$(cat \"$D/pid\")\" 2>/dev/null; then\n")
	b.WriteString("  echo 'a deploy is already running on this server: attaching to it'\n  exit 0\nfi\n")
	b.WriteString("rm -f \"$D/rc\" \"$D/pid\" \"$D/log\" \"$D/env\" \"$D/install.sh\"\n")
	b.WriteString("if ! command -v curl >/dev/null 2>&1; then\n")
	b.WriteString("  echo 'curl is missing: installing it'\n")
	b.WriteString("  (apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl) >/dev/null 2>&1 || dnf install -y -q curl >/dev/null 2>&1 || yum install -y -q curl >/dev/null 2>&1 || true\n")
	b.WriteString("  command -v curl >/dev/null 2>&1 || { echo 'curl is not installed and could not be installed' >&2; exit 3; }\nfi\n")
	fmt.Fprintf(&b, "curl -fsSL --retry 3 --retry-delay 2 %s -o \"$D/install.sh\" || { echo 'could not download the install script from %s' >&2; exit 4; }\n",
		shellQuote(cacheBust(scriptURL)), strings.ReplaceAll(scriptURL, "'", ""))
	b.WriteString("cat > \"$D/env\" <<'OPENFLUX_ENV'\n")
	b.WriteString(buildEnvFile(opts))
	b.WriteString("OPENFLUX_ENV\n")
	b.WriteString("nohup setsid bash -c '")
	b.WriteString("echo $$ > " + remoteDir + "/pid; . " + remoteDir + "/env; rm -f " + remoteDir + "/env; ")
	b.WriteString("bash " + remoteDir + "/install.sh; echo $? > " + remoteDir + "/rc")
	b.WriteString("' > \"$D/log\" 2>&1 < /dev/null &\n")
	b.WriteString("for i in $(seq 1 50); do [ -s \"$D/pid\" ] && break; sleep 0.1; done\n")
	b.WriteString("echo 'install started on the server (it keeps running if the connection drops)'\n")
	return b.String()
}

func buildPollScript(offset int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "D=%s\n", remoteDir)
	b.WriteString("if [ -f \"$D/rc\" ]; then echo \"STATUS done $(cat \"$D/rc\")\"\n")
	b.WriteString("elif [ -s \"$D/pid\" ] && kill -0 \"$(cat \"$D/pid\")\" 2>/dev/null; then echo 'STATUS running'\n")
	b.WriteString("elif [ -e \"$D/pid\" ]; then echo 'STATUS dead'\n")
	b.WriteString("else echo 'STATUS none'; fi\n")
	fmt.Fprintf(&b, "tail -c +%d \"$D/log\" 2>/dev/null | head -c %d\n", offset+1, pollChunkBytes)
	return b.String()
}

func cacheBust(scriptURL string) string {
	sep := "?"
	if strings.Contains(scriptURL, "?") {
		sep = "&"
	}
	return fmt.Sprintf("%s%s_=%d", scriptURL, sep, time.Now().UnixNano())
}

func boolToYN(b bool) string {
	if b {
		return "y"
	}
	return "n"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

type noopCallback struct{}

func (noopCallback) OnLog(string)                          {}
func (noopCallback) OnHostKeyFingerprint(string)           {}
func (noopCallback) OnDeployResult(string, string, string) {}
