#!/usr/bin/env python3
"""Builds the exit-node binaries, signs a release index and publishes both to the rolling "node-stable" release.

    python deploy/publish-node-release.py 0.1.0 "what changed" --key /path/to/update_ed25519.key

The nodes follow node-update.json on that release; a node accepts it only when the detached signature verifies under the
public key pinned in selfupdate/selfupdate.go, so the private key never leaves the machine that publishes. GitHub access
comes from GITHUB_TOKEN or, failing that, from git's stored credential for github.com.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

REPO = "wlruscfd/openflux-server"
TAG = "node-stable"
PLATFORMS = [("linux", "amd64"), ("linux", "arm64")]
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def token():
    t = os.environ.get("GITHUB_TOKEN")
    if t:
        return t
    out = subprocess.run(["git", "credential", "fill"], input="protocol=https\nhost=github.com\n\n",
                         capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        if line.startswith("password="):
            return line[len("password="):]
    sys.exit("no GitHub credential found")


def api(method, url, tok, body=None, raw=None, ctype="application/json"):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "Content-Type": ctype})
    try:
        with urllib.request.urlopen(req) as r:
            payload = r.read()
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        sys.exit("%s %s -> %d %s" % (method, url, e.code, e.read().decode()[:300]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("version")
    ap.add_argument("notes", nargs="?", default="")
    ap.add_argument("--key", required=True)
    ap.add_argument("--out", default=os.path.join(ROOT, "dist-node"))
    ap.add_argument("--dry-run", action="store_true", help="build and sign only")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    ldflags = "-s -w -X github.com/p1neappleXpress/OpenFlux/selfupdate.Version=" + args.version
    assets = {}
    files = []
    for goos, goarch in PLATFORMS:
        name = "openflux-node-%s-%s-%s" % (goos, goarch, args.version)
        path = os.path.join(args.out, name)
        env = dict(os.environ, CGO_ENABLED="0", GOOS=goos, GOARCH=goarch)
        subprocess.run(["go", "build", "-trimpath", "-ldflags", ldflags, "-o", path, "./cmd/openflux-node"],
                       cwd=ROOT, env=env, check=True)
        with open(path, "rb") as f:
            sha = hashlib.sha256(f.read()).hexdigest()
        assets["%s-%s" % (goos, goarch)] = {"url": "https://github.com/%s/releases/download/%s/%s" % (REPO, TAG, name), "sha256": sha}
        files.append((name, path, "application/octet-stream"))

    index = json.dumps({"format": 1, "version": args.version, "notes": args.notes, "assets": assets},
                       separators=(",", ":"), sort_keys=True).encode()
    index_path = os.path.join(args.out, "node-update.json")
    with open(index_path, "wb") as f:
        f.write(index)
    sig = subprocess.run(["go", "run", "./cmd/updatesign", "sign", args.key, index_path], cwd=ROOT,
                         capture_output=True, text=True, check=True).stdout.strip()
    sig_path = index_path + ".sig"
    with open(sig_path, "w") as f:
        f.write(sig)
    files += [("node-update.json", index_path, "application/json"), ("node-update.json.sig", sig_path, "text/plain")]

    if args.dry_run:
        print("built and signed in", args.out)
        return
    tok = token()
    base = "https://api.github.com/repos/" + REPO
    rel = api("GET", base + "/releases/tags/" + TAG, tok)
    if rel is None:
        rel = api("POST", base + "/releases", tok, {"tag_name": TAG, "name": "Exit node (rolling)", "target_commitish": "main",
                                                    "body": "Signed exit-node binaries and the update index the nodes follow.", "prerelease": False})
    # Index and signature go last, so a node never sees an index whose binaries are not uploaded yet.
    ordered = [f for f in files if f[0].startswith("openflux-node-")] + [f for f in files if not f[0].startswith("openflux-node-")]
    upload = rel["upload_url"].split("{")[0]
    old = {a["name"]: a["id"] for a in rel.get("assets", [])}
    for name, path, ctype in ordered:
        if name in old:
            api("DELETE", base + "/releases/assets/%d" % old[name], tok)
        with open(path, "rb") as f:
            api("POST", "%s?name=%s" % (upload, name), tok, raw=f.read(), ctype=ctype)
        print("uploaded", name)
    for name, aid in old.items():
        if name.startswith("openflux-node-") and not name.endswith(args.version):
            api("DELETE", base + "/releases/assets/%d" % aid, tok)
            print("removed", name)
    print("published", args.version)


if __name__ == "__main__":
    main()
