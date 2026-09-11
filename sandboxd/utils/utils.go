// Package utils holds helpers shared across sandboxd packages.
package utils

import (
	json "encoding/json/v2"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

// CloseWrite signals EOF to the peer without tearing down the read direction.
func CloseWrite(conn net.Conn) {
	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// WriteFileSync durably replaces path with data (temp + fsync + rename + dir fsync).
func WriteFileSync(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmp := f.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmp) //nolint:gosec // our own just-created temp under a caller-owned data-dir path
		}
	}()
	if err = f.Chmod(perm); err != nil {
		_ = f.Close()
		return fmt.Errorf("chmod %s: %w", tmp, err)
	}
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
	if err = os.Rename(tmp, path); err != nil { //nolint:gosec // our own temp, caller-owned data-dir path
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
