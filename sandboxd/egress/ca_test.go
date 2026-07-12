package egress

import (
	"crypto/x509"
	"testing"
)

func testCA(t *testing.T) (*CA, []byte) {
	t.Helper()
	rootCert, rootKey, err := GenerateRoot("test root")
	if err != nil {
		t.Fatalf("generate root: %v", err)
	}
	interCert, interKey, err := IssueIntermediate(rootCert, rootKey, "node1")
	if err != nil {
		t.Fatalf("issue intermediate: %v", err)
	}
	ca, err := LoadCA(rootCert, interCert, interKey)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	return ca, rootCert
}

func TestLoadCARejectsForeignIntermediate(t *testing.T) {
	rootA, keyA, err := GenerateRoot("A")
	if err != nil {
		t.Fatalf("root A: %v", err)
	}
	rootB, _, err := GenerateRoot("B")
	if err != nil {
		t.Fatalf("root B: %v", err)
	}
	interCert, interKey, err := IssueIntermediate(rootA, keyA, "n")
	if err != nil {
		t.Fatalf("intermediate: %v", err)
	}
	if _, err := LoadCA(rootB, interCert, interKey); err == nil {
		t.Error("LoadCA accepted an intermediate not signed by the given root")
	}
}

func TestLoadCARejectsRootAsIntermediate(t *testing.T) {
	rootCert, rootKey, err := GenerateRoot("root")
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	if _, err := LoadCA(rootCert, rootCert, rootKey); err == nil {
		t.Error("LoadCA accepted the root as its own intermediate; want rejection (would put the root key on the node)")
	}
}

func TestSignLeafChainsToRoot(t *testing.T) {
	ca, rootPEM := testCA(t)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		t.Fatal("append root")
	}
	for _, host := range []string{"api.github.com", "192.0.2.10"} {
		t.Run(host, func(t *testing.T) {
			crt, err := ca.SignLeaf(host)
			if err != nil {
				t.Fatalf("sign leaf: %v", err)
			}
			if len(crt.Certificate) != 2 {
				t.Fatalf("chain has %d certs, want 2 (leaf + intermediate)", len(crt.Certificate))
			}
			leaf, err := x509.ParseCertificate(crt.Certificate[0])
			if err != nil {
				t.Fatalf("parse leaf: %v", err)
			}
			inter, err := x509.ParseCertificate(crt.Certificate[1])
			if err != nil {
				t.Fatalf("parse intermediate: %v", err)
			}
			inters := x509.NewCertPool()
			inters.AddCert(inter)
			opts := x509.VerifyOptions{Roots: roots, Intermediates: inters, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
			if _, err := leaf.Verify(opts); err != nil {
				t.Errorf("verify chain to root: %v", err)
			}
			if err := leaf.VerifyHostname(host); err != nil {
				t.Errorf("verify hostname %s: %v", host, err)
			}
		})
	}
}

func TestFingerprintIsRootAndStable(t *testing.T) {
	rootCert, rootKey, err := GenerateRoot("root")
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	c1, k1, _ := IssueIntermediate(rootCert, rootKey, "n1")
	c2, k2, _ := IssueIntermediate(rootCert, rootKey, "n2")
	ca1, err := LoadCA(rootCert, c1, k1)
	if err != nil {
		t.Fatalf("load ca1: %v", err)
	}
	ca2, err := LoadCA(rootCert, c2, k2)
	if err != nil {
		t.Fatalf("load ca2: %v", err)
	}
	if ca1.Fingerprint() != ca2.Fingerprint() {
		t.Error("fingerprint differs across nodes sharing a root; must track the root only")
	}
}
