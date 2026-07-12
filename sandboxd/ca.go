package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cocoonstack/sandbox/sandboxd/egress"
)

// runCA is the operator PKI tool for HTTPS interception: mint the cluster root
// once, then issue a per-node intermediate from it. The root key stays with the
// operator; only the intermediate (and the root cert) are provisioned to a node.
func runCA(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: sandboxd ca {init|issue-intermediate}")
	}
	switch args[0] {
	case "init":
		return caInit(args[1:])
	case "issue-intermediate":
		return caIssueIntermediate(args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q (want init|issue-intermediate)", args[0])
	}
}

func caInit(args []string) error {
	fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
	out := fs.String("out", ".", "directory to write root.crt and root.key")
	cn := fs.String("cn", "sandbox egress root", "root certificate common name")
	force := fs.Bool("force", false, "overwrite existing files")
	if err := fs.Parse(args); err != nil {
		return err
	}
	certPEM, keyPEM, err := egress.GenerateRoot(*cn)
	if err != nil {
		return err
	}
	if err := writeCAFiles(*out, "root", certPEM, keyPEM, *force); err != nil {
		return err
	}
	fmt.Printf("wrote %s/root.crt and %s/root.key — keep root.key offline; never provision it to a node\n", *out, *out)
	return nil
}

func caIssueIntermediate(args []string) error {
	fs := flag.NewFlagSet("ca issue-intermediate", flag.ContinueOnError)
	rootCert := fs.String("root-cert", "", "cluster root certificate")
	rootKey := fs.String("root-key", "", "cluster root private key")
	node := fs.String("node", "", "node id the intermediate is issued for")
	out := fs.String("out", ".", "directory to write <node>.crt and <node>.key")
	force := fs.Bool("force", false, "overwrite existing files")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rootCert == "" || *rootKey == "" || *node == "" {
		return fmt.Errorf("-root-cert, -root-key, and -node are required")
	}
	rc, err := os.ReadFile(*rootCert)
	if err != nil {
		return fmt.Errorf("read root cert: %w", err)
	}
	rk, err := os.ReadFile(*rootKey)
	if err != nil {
		return fmt.Errorf("read root key: %w", err)
	}
	certPEM, keyPEM, err := egress.IssueIntermediate(rc, rk, *node)
	if err != nil {
		return err
	}
	if err := writeCAFiles(*out, *node, certPEM, keyPEM, *force); err != nil {
		return err
	}
	fmt.Printf("wrote %s/%s.crt and %s/%s.key — provision both to node %q with the root cert\n", *out, *node, *out, *node, *node)
	return nil
}

func writeCAFiles(dir, name string, certPEM, keyPEM []byte, force bool) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	certPath := filepath.Join(dir, name+".crt")
	keyPath := filepath.Join(dir, name+".key")
	if !force {
		for _, p := range []string{certPath, keyPath} {
			if _, err := os.Stat(p); err == nil {
				return fmt.Errorf("%s exists; refusing to overwrite (use -force)", p)
			}
		}
	}
	if err := os.WriteFile(certPath, certPEM, 0o644); err != nil { //nolint:gosec // public cert
		return fmt.Errorf("write cert: %w", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}
	// WriteFile applies the mode only on create; force-overwriting an existing
	// key must not inherit looser bits.
	if err := os.Chmod(keyPath, 0o600); err != nil {
		return fmt.Errorf("chmod key: %w", err)
	}
	return nil
}
