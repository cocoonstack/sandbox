package egress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

const (
	leafValidity = 90 * 24 * time.Hour
	clockSkew    = time.Hour
)

// CA signs per-host interception leaves with the node's intermediate. The
// shared cluster root is baked into guests; the root key never reaches a node.
type CA struct {
	rootPEM   []byte
	rootFP    string
	interCert *x509.Certificate
	interDER  []byte
	interKey  *ecdsa.PrivateKey
	leafKey   *ecdsa.PrivateKey
}

// LoadCA builds the node CA; the intermediate must be signed by the bundle's
// first root cert (a bundle carries old+new roots during a rotation).
func LoadCA(rootCertPEM, interCertPEM, interKeyPEM []byte) (*CA, error) {
	root, err := parseRootBundle(rootCertPEM)
	if err != nil {
		return nil, err
	}
	if err = checkValidity(root, "root"); err != nil {
		return nil, err
	}
	inter, err := parseCert(interCertPEM, "intermediate")
	if err != nil {
		return nil, err
	}
	if err = checkValidity(inter, "intermediate"); err != nil {
		return nil, err
	}
	interKey, err := parseECKey(interKeyPEM)
	if err != nil {
		return nil, err
	}
	if err = inter.CheckSignatureFrom(root); err != nil {
		return nil, fmt.Errorf("intermediate not signed by root: %w", err)
	}
	// Reject a self-signed intermediate: the root must not double as one, which
	// would put the root key on the node.
	if inter.CheckSignatureFrom(inter) == nil {
		return nil, fmt.Errorf("intermediate must not be a self-signed root")
	}
	if !interKey.PublicKey.Equal(inter.PublicKey) {
		return nil, fmt.Errorf("intermediate key does not match its cert")
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate leaf key: %w", err)
	}
	// Fingerprint the whole file: CertPEM bakes it verbatim.
	sum := sha256.Sum256(rootCertPEM)
	return &CA{
		rootPEM:   rootCertPEM,
		rootFP:    hex.EncodeToString(sum[:]),
		interCert: inter,
		interDER:  inter.Raw,
		interKey:  interKey,
		leafKey:   leafKey,
	}, nil
}

// CertPEM is the cluster root to bake into an intercepted guest's trust store.
func (c *CA) CertPEM() []byte { return c.rootPEM }

// Fingerprint is the SHA-256 of the baked root PEM; changed bytes rebuild the
// golden, a rotated intermediate does not.
func (c *CA) Fingerprint() string { return c.rootFP }

// SignLeaf issues a fresh interception leaf for host, chained [leaf,
// intermediate] for a guest that trusts the root.
func (c *CA) SignLeaf(host string) (*tls.Certificate, error) {
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(leafValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.interCert, &c.leafKey.PublicKey, c.interKey)
	if err != nil {
		return nil, fmt.Errorf("sign leaf %s: %w", host, err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse leaf %s: %w", host, err)
	}
	return &tls.Certificate{Certificate: [][]byte{der, c.interDER}, PrivateKey: c.leafKey, Leaf: leaf}, nil
}

func parseCert(pemBytes []byte, what string) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("%s cert: no pem block", what)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse %s cert: %w", what, err)
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("%s cert: not a certificate authority", what)
	}
	return cert, nil
}

// parseRootBundle returns the first cert (the anchor) and rejects any non-CA
// block: the whole file lands in guest trust stores.
func parseRootBundle(pemBytes []byte) (*x509.Certificate, error) {
	var anchor *x509.Certificate
	for rest := pemBytes; ; {
		var block *pem.Block
		if block, rest = pem.Decode(rest); block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("root bundle: unexpected %s pem block", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse root bundle cert: %w", err)
		}
		if !cert.IsCA {
			return nil, fmt.Errorf("root bundle: %q is not a certificate authority", cert.Subject.CommonName)
		}
		if anchor == nil {
			anchor = cert
		}
	}
	if anchor == nil {
		return nil, fmt.Errorf("root cert: no pem block")
	}
	return anchor, nil
}

func checkValidity(cert *x509.Certificate, what string) error {
	now := time.Now()
	if now.Before(cert.NotBefore) {
		return fmt.Errorf("%s cert not valid until %s", what, cert.NotBefore.Format(time.RFC3339))
	}
	if now.After(cert.NotAfter) {
		return fmt.Errorf("%s cert expired %s", what, cert.NotAfter.Format(time.RFC3339))
	}
	return nil
}

func parseECKey(pemBytes []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("intermediate key: no pem block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse intermediate key: %w", err)
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("intermediate key: want ecdsa, got %T", parsed)
	}
	return key, nil
}

func serialNumber() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}
	return n, nil
}
