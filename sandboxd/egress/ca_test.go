package egress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"
	"time"
)

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

func TestLoadCARejectsExpiredIntermediate(t *testing.T) {
	rootCert, rootKey, err := GenerateRoot("root")
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	root, err := parseCert(rootCert, "root")
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}
	rk, err := parseECKey(rootKey)
	if err != nil {
		t.Fatalf("parse root key: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	serial, err := serialNumber()
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "expired intermediate"},
		NotBefore:             time.Now().Add(-2 * time.Hour),
		NotAfter:              time.Now().Add(-time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, root, &key.PublicKey, rk)
	if err != nil {
		t.Fatalf("sign expired intermediate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if _, err := LoadCA(rootCert, certPEM, keyPEM); err == nil {
		t.Error("LoadCA accepted an expired intermediate")
	}
}

func TestLoadCARootBundle(t *testing.T) {
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
	bundle := append(append([]byte{}, rootA...), rootB...)
	ca, err := LoadCA(bundle, interCert, interKey)
	if err != nil {
		t.Fatalf("LoadCA rotation bundle: %v", err)
	}
	single, err := LoadCA(rootA, interCert, interKey)
	if err != nil {
		t.Fatalf("LoadCA single root: %v", err)
	}
	if ca.Fingerprint() == single.Fingerprint() {
		t.Error("fingerprint ignores the bundled second root; .cafp would not rebuild goldens")
	}
	if string(ca.CertPEM()) != string(bundle) {
		t.Error("CertPEM must bake the whole bundle verbatim")
	}
}

func TestLoadCARejectsNonCertBlockInRoot(t *testing.T) {
	rootCert, rootKey, err := GenerateRoot("root")
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	interCert, interKey, err := IssueIntermediate(rootCert, rootKey, "n")
	if err != nil {
		t.Fatalf("intermediate: %v", err)
	}
	polluted := append(append([]byte{}, rootCert...), rootKey...)
	if _, err := LoadCA(polluted, interCert, interKey); err == nil {
		t.Error("LoadCA accepted a non-certificate pem block in the root bundle")
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
