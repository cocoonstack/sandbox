// Package utils holds helpers shared across sandboxd packages.
package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeStrictJSON decodes one JSON value into v, refusing unknown fields,
// duplicated keys, and trailing data — for hand-edited operator input, where
// a typo must fail instead of silently changing what was configured (an
// unknown or repeated key is dropped or last-wins under a lenient decode).
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

// rejectDuplicateKeys walks one JSON value and refuses objects that repeat a
// key at the same level.
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
			key, _ := keyTok.(string)
			if _, dup := seen[key]; dup {
				return fmt.Errorf("duplicate key %q", key)
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
