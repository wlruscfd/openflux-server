package mobile

import (
	"encoding/json"
	"fmt"

	"github.com/p1neappleXpress/OpenFlux/deployssh"
)

// Deploy takes JSON strings rather than gomobile-bound structs: gomobile silently drops any exported function taking a custom struct parameter.
type sshTargetJSON struct {
	Host                    string `json:"host"`
	Port                    int    `json:"port"`
	Username                string `json:"username"`
	AuthMethod              string `json:"auth_method"`
	Password                string `json:"password"`
	PrivateKeyPEM           string `json:"private_key_pem"`
	Passphrase              string `json:"passphrase"`
	KnownHostKeyFingerprint string `json:"known_host_key_fingerprint"`
}

type deployOptionsJSON struct {
	DeployScriptURL string `json:"deploy_script_url"`
	RepoURL         string `json:"repo_url"`
	GitRef          string `json:"git_ref"`
	TLSMode         string `json:"tls_mode"`
	Domain          string `json:"domain"`
	Email           string `json:"email"`
	ServerIP        string `json:"server_ip"`
	AdminToken      string `json:"admin_token"`
	DBPassword      string `json:"db_password"`
	RegisterNode    bool   `json:"register_node"`
	NodeName        string `json:"node_name"`
	NodeMaxKeys     int    `json:"node_max_keys"`
	RunNodeHere     bool   `json:"run_node_here"`
}

type DeployCallback interface {
	OnLog(line string)
	OnHostKeyFingerprint(fingerprint string)
	OnDeployResult(panelURL, adminToken, nodeToken string)
}

func Deploy(targetJSON string, optsJSON string, cb DeployCallback) error {
	var target sshTargetJSON
	if err := json.Unmarshal([]byte(targetJSON), &target); err != nil {
		return fmt.Errorf("parse target: %w", err)
	}
	var opts deployOptionsJSON
	if err := json.Unmarshal([]byte(optsJSON), &opts); err != nil {
		return fmt.Errorf("parse options: %w", err)
	}

	return deployssh.Deploy(
		deployssh.SSHTarget{
			Host:                    target.Host,
			Port:                    target.Port,
			Username:                target.Username,
			AuthMethod:              target.AuthMethod,
			Password:                target.Password,
			PrivateKeyPEM:           target.PrivateKeyPEM,
			Passphrase:              target.Passphrase,
			KnownHostKeyFingerprint: target.KnownHostKeyFingerprint,
		},
		deployssh.DeployOptions{
			DeployScriptURL: opts.DeployScriptURL,
			RepoURL:         opts.RepoURL,
			GitRef:          opts.GitRef,
			TLSMode:         opts.TLSMode,
			Domain:          opts.Domain,
			Email:           opts.Email,
			ServerIP:        opts.ServerIP,
			AdminToken:      opts.AdminToken,
			DBPassword:      opts.DBPassword,
			RegisterNode:    opts.RegisterNode,
			NodeName:        opts.NodeName,
			NodeMaxKeys:     opts.NodeMaxKeys,
			RunNodeHere:     opts.RunNodeHere,
		},
		deployCallbackAdapter{cb},
	)
}

type deployCallbackAdapter struct {
	cb DeployCallback
}

func (a deployCallbackAdapter) OnLog(line string) {
	if a.cb != nil {
		a.cb.OnLog(line)
	}
}

func (a deployCallbackAdapter) OnHostKeyFingerprint(fingerprint string) {
	if a.cb != nil {
		a.cb.OnHostKeyFingerprint(fingerprint)
	}
}

func (a deployCallbackAdapter) OnDeployResult(panelURL, adminToken, nodeToken string) {
	if a.cb != nil {
		a.cb.OnDeployResult(panelURL, adminToken, nodeToken)
	}
}
