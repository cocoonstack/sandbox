// Package utils holds helpers shared across sandboxd packages.
package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	defer func() { _ = d.Close() }()
	if err = d.Sync(); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}
	return nil
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

// DecodeStrictJSON decodes one JSON value into v, refusing unknown fields and duplicates.
func DecodeStrictJSON(raw []byte, v any) error {
	if err := rejectDuplicateKeys(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing data after the value")
	}
	return nil
}

// rejectDuplicateKeys refuses objects that repeat a key at the same level.
func rejectDuplicateKeys(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for dec.More() {
			keyTok, keyErr := dec.Token()
			if keyErr != nil {
				return keyErr
			}
			// encoding/json matches struct fields case-insensitively, so fold case here.
			keyStr, _ := keyTok.(string)
			key := strings.ToLower(keyStr)
			if _, dup := seen[key]; dup {
				return fmt.Errorf("duplicate key %q", keyStr)
			}
			seen[key] = struct{}{}
			if walkErr := rejectDuplicateKeys(dec); walkErr != nil {
				return walkErr
			}
		}
		_, err = dec.Token()
		return err
	case '[':
		for dec.More() {
			if walkErr := rejectDuplicateKeys(dec); walkErr != nil {
				return walkErr
			}
		}
		_, err = dec.Token()
		return err
	}
	return nil
}
