// Package utils holds helpers shared across sandboxd packages.
package utils

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) {
	if c.r.Buffered() > 0 {
		return c.r.Read(b)
	}
	return c.Conn.Read(b)
}

// CloseWrite signals EOF to the peer without tearing down the read direction.
func CloseWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// WithBuffered returns c reading r's buffered bytes first; c itself when r holds none.
func WithBuffered(c net.Conn, r *bufio.Reader) net.Conn {
	if r.Buffered() == 0 {
		return c
	}
	return &bufferedConn{Conn: c, r: r}
}

// WriteFileSync durably replaces path with data (temp + fsync + rename + dir fsync).
func WriteFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	if err = f.Chmod(perm); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return fmt.Errorf("chmod %s: %w", f.Name(), err)
	}
	return ReplaceFileSync(f, path, data)
}

// ReplaceFileSync writes data to the open temp file f, fsyncs it, renames it over path and fsyncs the directory; f is closed and removed on failure.
func ReplaceFileSync(f *os.File, path string, data []byte) (err error) {
	dir, tmp := filepath.Dir(path), f.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", path, err)
	}
	renamed = true
	d, err := os.Open(dir) //nolint:gosec // caller-owned data-dir path
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	if err = d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync dir: %w", err)
	}
	return d.Close()
}

// RemoveDirEntries removes dir's entries for which match returns true; nil matches all.
func RemoveDirEntries(dir string, match func(name string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if match != nil && !match(e.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// DecodeStrictJSON decodes one JSON value into v, refusing unknown fields, duplicates and trailing data.
func DecodeStrictJSON(raw []byte, v any) error {
	return json.Unmarshal(raw, v, json.RejectUnknownMembers(true), json.MatchCaseInsensitiveNames(true))
}

// DigestJSON encodes v with sorted map keys, so equal values always digest equal.
func DigestJSON(v any) ([]byte, error) {
	return json.Marshal(v, json.Deterministic(true))
}

// DigestHex is the hex SHA-256 of v's DigestJSON bytes.
func DigestHex(v any) string {
	raw, _ := DigestJSON(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
