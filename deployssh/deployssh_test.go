package deployssh

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"plain":     `'plain'`,
		"has space": `'has space'`,
		"":          `''`,
		"a'b":       `'a'\''b'`,
		"a'b'c":     `'a'\''b'\''c'`,
	}
	for in, want := range cases {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildEnvFileQuotesAndKeepsExplicitNegatives(t *testing.T) {
	env := buildEnvFile(DeployOptions{
		RepoURL:      "https://github.com/wlruscfd/openflux-server.git",
		GitRef:       "main",
		TLSMode:      "domain",
		Domain:       "panel.example.com",
		Email:        "you@example.com",
		AdminToken:   "adm'in",
		DBPassword:   "dbpass",
		RegisterNode: false,
	})

	for _, want := range []string{
		`export ADMIN_TOKEN='adm'\''in'`,
		"export REGISTER_NODE='n'",
		"export RUN_NODE_HERE='n'",
		"export WEB_PANEL='y'",
		"export TLS_MODE='domain'",
	} {
		if !strings.Contains(env, want) {
			t.Errorf("env file should contain %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "SERVER_IP") {
		t.Errorf("SERVER_IP is blank in domain mode and should not appear at all:\n%s", env)
	}
}

func TestPlainHTTPModeKeepsEmbeddedPanel(t *testing.T) {
	if env := buildEnvFile(DeployOptions{TLSMode: "http"}); !strings.Contains(env, "export WEB_PANEL='n'") {
		t.Errorf("http mode serves the controlplane directly and must not ask for the web panel: %s", env)
	}
}

func TestStartScriptFetchesCacheBustedScriptAndDetaches(t *testing.T) {
	script := buildStartScript(DeployOptions{})
	if !strings.Contains(script, "curl -fsSL --retry 3 --retry-delay 2 '"+defaultDeployScriptURL+"?_=") {
		t.Errorf("start script should fetch the default script URL, cache-busted:\n%s", script)
	}
	if !strings.Contains(script, "nohup setsid bash -c") {
		t.Errorf("the install must be detached from the ssh session so a dropped connection cannot kill it:\n%s", script)
	}

	custom := buildStartScript(DeployOptions{DeployScriptURL: "https://example.com/my-install.sh"})
	if !strings.Contains(custom, "curl -fsSL --retry 3 --retry-delay 2 'https://example.com/my-install.sh?_=") {
		t.Errorf("a custom DeployScriptURL should be used, cache-busted the same way:\n%s", custom)
	}
}

func TestPollScriptResumesFromOffset(t *testing.T) {
	if script := buildPollScript(0); !strings.Contains(script, "tail -c +1 ") {
		t.Errorf("offset 0 should read from the first byte:\n%s", script)
	}
	if script := buildPollScript(99); !strings.Contains(script, "tail -c +100 ") {
		t.Errorf("offset 99 should resume at byte 100:\n%s", script)
	}
}

func TestCacheBustAppendsQueryParamCorrectly(t *testing.T) {
	if got := cacheBust("https://example.com/install.sh"); !strings.HasPrefix(got, "https://example.com/install.sh?_=") {
		t.Errorf("cacheBust(no existing query) = %q, want a ?_= param appended", got)
	}
	if got := cacheBust("https://example.com/install.sh?ref=main"); !strings.HasPrefix(got, "https://example.com/install.sh?ref=main&_=") {
		t.Errorf("cacheBust(existing query) = %q, want an &_= param appended", got)
	}
}

// --- real local SSH server tests -----------------------------------------

// --- real local SSH server tests -----------------------------------------

type testCallback struct {
	mu          sync.Mutex
	lines       []string
	fingerprint string
	result      *deployResult
}

type deployResult struct {
	panelURL, adminToken, nodeToken string
}

func (c *testCallback) OnLog(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
}

func (c *testCallback) OnHostKeyFingerprint(fp string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fingerprint = fp
}

func (c *testCallback) OnDeployResult(panelURL, adminToken, nodeToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.result = &deployResult{panelURL: panelURL, adminToken: adminToken, nodeToken: nodeToken}
}

func (c *testCallback) Lines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *testCallback) Result() *deployResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.result
}

func generateHostSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatalf("signer from host key: %v", err)
	}
	return signer
}

func generateRSAClientKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	der := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der})

	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("signer from rsa key: %v", err)
	}
	return signer.PublicKey(), string(pemBytes)
}

func startTestSSHServer(t *testing.T, config *ssh.ServerConfig, hostSigner ssh.Signer, handle func(cmd string, stdout, stderr io.Writer) int) string {
	t.Helper()
	config.AddHostKey(hostSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			nConn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveOneConn(nConn, config, handle)
		}
	}()

	return listener.Addr().String()
}

func serveOneConn(nConn net.Conn, config *ssh.ServerConfig, handle func(cmd string, stdout, stderr io.Writer) int) {
	sshConn, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		return // auth failure etc - expected for the negative-path tests
	}
	defer sshConn.Close()
	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go func() {
			defer channel.Close()
			for req := range requests {
				if req.Type != "exec" {
					req.Reply(false, nil)
					continue
				}
				var payload struct{ Command string }
				ssh.Unmarshal(req.Payload, &payload)
				req.Reply(true, nil)

				code := handle(payload.Command, channel, channel.Stderr())
				channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(code)}))
				return
			}
		}()
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port %q: %v", addr, err)
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

type fakeHost struct {
	mu          sync.Mutex
	uid         string
	log         string
	rc          string
	running     bool
	started     int
	startCmd    string
	polls       int
	failStart   bool
	dropPollsAt map[int]bool
	script      func(h *fakeHost)
}

func (h *fakeHost) handle(cmd string, stdout, stderr io.Writer) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	cmd = strings.TrimPrefix(cmd, "sudo -S -p '' ")
	switch {
	case cmd == "id -u":
		uid := h.uid
		if uid == "" {
			uid = "0"
		}
		fmt.Fprintln(stdout, uid)
		return 0
	case cmd == "true":
		return 0
	case strings.Contains(cmd, "OPENFLUX_ENV"):
		if h.failStart {
			fmt.Fprintln(stderr, "could not download the install script")
			return 4
		}
		h.started++
		h.startCmd = strings.ReplaceAll(cmd, `'\''`, "'")
		h.running = true
		h.log = ""
		h.rc = ""
		if h.script != nil {
			go h.script(h)
		}
		fmt.Fprintln(stdout, "install started on the server")
		return 0
	case strings.Contains(cmd, "STATUS"):
		h.polls++
		if h.dropPollsAt[h.polls] {
			return -1
		}
		offset := 0
		if i := strings.Index(cmd, "tail -c +"); i >= 0 {
			rest := cmd[i+len("tail -c +"):]
			end := strings.IndexByte(rest, ' ')
			offset, _ = strconv.Atoi(rest[:end])
			offset--
		}
		switch {
		case h.rc != "":
			fmt.Fprintf(stdout, "STATUS done %s\n", h.rc)
		case h.running:
			fmt.Fprintln(stdout, "STATUS running")
		case h.started > 0:
			fmt.Fprintln(stdout, "STATUS dead")
		default:
			fmt.Fprintln(stdout, "STATUS none")
		}
		if offset < len(h.log) {
			fmt.Fprint(stdout, h.log[offset:])
		}
		return 0
	}
	fmt.Fprintf(stderr, "unexpected command: %s\n", cmd)
	return 127
}

func (h *fakeHost) appendLog(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.log += s
}

func (h *fakeHost) finish(rc string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rc = rc
	h.running = false
}

func (h *fakeHost) die() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = false
}

func fastTimers(t *testing.T) {
	t.Helper()
	oldPoll, oldDelay, oldWindow, oldFollow := pollInterval, reconnectDelay, reconnectWindow, followTimeout
	pollInterval = 5 * time.Millisecond
	reconnectDelay = 10 * time.Millisecond
	reconnectWindow = 2 * time.Second
	followTimeout = 10 * time.Second
	t.Cleanup(func() {
		pollInterval, reconnectDelay, reconnectWindow, followTimeout = oldPoll, oldDelay, oldWindow, oldFollow
	})
}

func passwordConfig(user, pass string) *ssh.ServerConfig {
	return &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, p []byte) (*ssh.Permissions, error) {
			if c.User() == user && string(p) == pass {
				return nil, nil
			}
			return nil, fmt.Errorf("denied")
		},
	}
}

func TestDeploySuccessStreamsOutputAndFingerprint(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{script: func(h *fakeHost) {
		h.appendLog("installing packages\n")
		time.Sleep(20 * time.Millisecond)
		h.appendLog("done\npartial")
		h.finish("0")
	}}
	addr := startTestSSHServer(t, passwordConfig("root", "correct-horse"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "correct-horse"}
	opts := DeployOptions{RepoURL: "https://x/y.git", GitRef: "main", TLSMode: "ip", ServerIP: "1.2.3.4", AdminToken: "tok"}

	if err := Deploy(target, opts, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	if want := ssh.FingerprintSHA256(hostSigner.PublicKey()); cb.fingerprint != want {
		t.Errorf("fingerprint = %q, want %q", cb.fingerprint, want)
	}
	joined := strings.Join(cb.Lines(), "\n")
	for _, want := range []string{"installing packages", "done", "partial"} {
		if !strings.Contains(joined, want) {
			t.Errorf("expected log output to contain %q, got: %v", want, cb.Lines())
		}
	}
	if !strings.Contains(h.startCmd, "export SERVER_IP='1.2.3.4'") {
		t.Errorf("start command should carry SERVER_IP for ip mode: %s", h.startCmd)
	}
}

func TestDeployWrongPasswordFails(t *testing.T) {
	hostSigner := generateHostSigner(t)
	addr := startTestSSHServer(t, passwordConfig("root", "right"), hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		t.Errorf("command should never run when auth fails")
		return 0
	})
	host, port := splitHostPort(t, addr)

	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "wrong"}
	if err := Deploy(target, DeployOptions{}, &testCallback{}); err == nil {
		t.Fatalf("expected an error for a rejected password")
	}
}

func TestDeployHostKeyMismatchRejectsBeforeRunningAnything(t *testing.T) {
	hostSigner := generateHostSigner(t)
	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	ran := false
	addr := startTestSSHServer(t, config, hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		ran = true
		return 0
	})
	host, port := splitHostPort(t, addr)

	target := SSHTarget{
		Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x",
		KnownHostKeyFingerprint: "SHA256:this-will-never-match-anything",
	}
	err := Deploy(target, DeployOptions{}, &testCallback{})
	if err == nil {
		t.Fatalf("expected an error for a mismatched host key")
	}
	if ran {
		t.Errorf("the remote command must not run when the host key doesn't match")
	}
}

func TestDeployKeyAuth(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	clientPub, clientPEM := generateRSAClientKey(t)

	config := &ssh.ServerConfig{
		PublicKeyCallback: func(c ssh.ConnMetadata, pubKey ssh.PublicKey) (*ssh.Permissions, error) {
			if bytes.Equal(pubKey.Marshal(), clientPub.Marshal()) {
				return nil, nil
			}
			return nil, fmt.Errorf("unauthorized key")
		},
	}
	h := &fakeHost{script: func(h *fakeHost) { h.finish("0") }}
	addr := startTestSSHServer(t, config, hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "key", PrivateKeyPEM: clientPEM}
	if err := Deploy(target, DeployOptions{}, &testCallback{}); err != nil {
		t.Fatalf("Deploy with key auth: %v", err)
	}
}

func TestDeployReportsResultLineAndHidesItFromLog(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{script: func(h *fakeHost) {
		h.appendLog("installing packages\n")
		h.appendLog("OPENFLUX_DEPLOY_RESULT panel_url=https://1.2.3.4/admin/ admin_token=admtok node_token=nodetok\n")
		h.appendLog("done\n")
		h.finish("0")
	}}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	result := cb.Result()
	if result == nil {
		t.Fatalf("OnDeployResult was never called")
	}
	if result.panelURL != "https://1.2.3.4/admin/" || result.adminToken != "admtok" || result.nodeToken != "nodetok" {
		t.Errorf("result = %+v", *result)
	}
	for _, line := range cb.Lines() {
		if strings.Contains(line, "OPENFLUX_DEPLOY_RESULT") {
			t.Errorf("the machine-readable result line should not also be reported via OnLog: %q", line)
		}
	}
}

func TestDeployNonZeroExitIsAnError(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{script: func(h *fakeHost) {
		h.appendLog("something went wrong\n")
		h.finish("1")
	}}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	err := Deploy(target, DeployOptions{}, cb)
	if err == nil || !strings.Contains(err.Error(), "exit code 1") {
		t.Fatalf("expected an exit code error, got %v", err)
	}
	if !strings.Contains(strings.Join(cb.Lines(), "\n"), "something went wrong") {
		t.Errorf("the failing install's output should still be streamed: %v", cb.Lines())
	}
}

func TestDeployKilledInstallIsAnError(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{script: func(h *fakeHost) {
		h.appendLog("working\n")
		h.die()
	}}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	err := Deploy(target, DeployOptions{}, &testCallback{})
	if err == nil || !strings.Contains(err.Error(), "interrupted") {
		t.Fatalf("expected an interrupted error, got %v", err)
	}
}

func TestDeployStartFailureShowsServerOutput(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{failStart: true}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, cb); err == nil {
		t.Fatalf("expected an error when the start script fails")
	}
	if !strings.Contains(strings.Join(cb.Lines(), "\n"), "could not download the install script") {
		t.Errorf("the start script's stderr should reach the log: %v", cb.Lines())
	}
}

func TestDeployUsesSudoForNonRootUser(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{uid: "1000", script: func(h *fakeHost) { h.finish("0") }}
	addr := startTestSSHServer(t, passwordConfig("ubuntu", "pw"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "ubuntu", AuthMethod: "password", Password: "pw"}
	if err := Deploy(target, DeployOptions{}, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if !strings.Contains(strings.Join(cb.Lines(), "\n"), "using sudo") {
		t.Errorf("a non-root user should be told sudo is used: %v", cb.Lines())
	}
}

func TestDeployResumesAfterDroppedConnection(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{
		dropPollsAt: map[int]bool{2: true},
		script: func(h *fakeHost) {
			h.appendLog("step one\n")
			time.Sleep(60 * time.Millisecond)
			h.appendLog("step two\n")
			h.finish("0")
		},
	}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, func(cmd string, stdout, stderr io.Writer) int {
		code := h.handle(cmd, stdout, stderr)
		if code == -1 {
			return 1
		}
		return code
	})
	host, port := splitHostPort(t, addr)

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{}, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	joined := strings.Join(cb.Lines(), "\n")
	if strings.Count(joined, "step one") != 1 || !strings.Contains(joined, "step two") {
		t.Errorf("each log line should arrive exactly once across a reconnect: %v", cb.Lines())
	}
}

func TestDeployAttachOnlyDoesNotStartAnything(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{running: true, started: 1, log: "already going\n"}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)
	go func() {
		time.Sleep(50 * time.Millisecond)
		h.appendLog("finished\n")
		h.finish("0")
	}()

	cb := &testCallback{}
	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	if err := Deploy(target, DeployOptions{AttachOnly: true}, cb); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if h.started != 1 {
		t.Errorf("attach must not start another install, started=%d", h.started)
	}
	joined := strings.Join(cb.Lines(), "\n")
	if !strings.Contains(joined, "already going") || !strings.Contains(joined, "finished") {
		t.Errorf("attach should replay the log from the start: %v", cb.Lines())
	}
}

func TestDeployAttachOnlyWithoutRunningInstallFails(t *testing.T) {
	fastTimers(t)
	hostSigner := generateHostSigner(t)
	h := &fakeHost{}
	addr := startTestSSHServer(t, passwordConfig("root", "x"), hostSigner, h.handle)
	host, port := splitHostPort(t, addr)

	target := SSHTarget{Host: host, Port: port, Username: "root", AuthMethod: "password", Password: "x"}
	err := Deploy(target, DeployOptions{AttachOnly: true}, &testCallback{})
	if err == nil || !strings.Contains(err.Error(), "no deploy is running") {
		t.Fatalf("expected a no-deploy error, got %v", err)
	}
}

func TestDeployEmptyHostIsRejected(t *testing.T) {
	if err := Deploy(SSHTarget{}, DeployOptions{}, &testCallback{}); err == nil {
		t.Fatalf("expected an error for an empty host")
	}
}
