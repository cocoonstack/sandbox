package egress

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"time"
)

const (
	rootValidity  = 10 * 365 * 24 * time.Hour
	interValidity = 5 * 365 * 24 * time.Hour
)

// GenerateRoot mints a cluster root CA (cert + key PEM). Run once per cluster;
// the key stays with the operator and never reaches a node.
func GenerateRoot(commonName string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate root key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(rootValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	return marshalCA(tmpl, tmpl, &key.PublicKey, key, key)
}

// IssueIntermediate signs a per-node intermediate CA (leaves only, path length
// zero) from the cluster root, to provision to nodeID.
func IssueIntermediate(rootCertPEM, rootKeyPEM []byte, nodeID string) (certPEM, keyPEM []byte, err error) {
	root, err := parseCert(rootCertPEM, "root")
	if err != nil {
		return nil, nil, err
	}
	rootKey, err := parseECKey(rootKeyPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("root key: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate intermediate key: %w", err)
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "sandbox egress intermediate " + nodeID},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(interValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	return marshalCA(tmpl, root, &key.PublicKey, rootKey, key)
}

func marshalCA(tmpl, parent *x509.Certificate, pub, signer, keyToPEM any) (certPEM, keyPEM []byte, err error) {
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
	if err != nil {
		return nil, nil, fmt.Errorf("create cert: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(keyToPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal key: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: pemTypeCertificate, Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}
