package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

const usage = `updatesign genkey <private-key-file>      make a signing key, print its public half
updatesign pub <private-key-file>         print the public half of an existing key
updatesign sign <private-key-file> <file> print the detached hex signature of a file`

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "genkey":
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		check(err)
		check(os.WriteFile(os.Args[2], []byte(hex.EncodeToString(priv.Seed())+"\n"), 0o600))
		fmt.Println(hex.EncodeToString(pub))
	case "pub":
		fmt.Println(hex.EncodeToString(load(os.Args[2]).Public().(ed25519.PublicKey)))
	case "sign":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, usage)
			os.Exit(2)
		}
		data, err := os.ReadFile(os.Args[3])
		check(err)
		fmt.Println(hex.EncodeToString(ed25519.Sign(load(os.Args[2]), data)))
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
}

func load(path string) ed25519.PrivateKey {
	raw, err := os.ReadFile(path)
	check(err)
	seed, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	check(err)
	if len(seed) != ed25519.SeedSize {
		check(fmt.Errorf("%s is not an ed25519 seed", path))
	}
	return ed25519.NewKeyFromSeed(seed)
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
