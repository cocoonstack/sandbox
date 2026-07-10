package utils

import "testing"

func TestDecodeStrictJSON(t *testing.T) {
	type target struct {
		Methods []string `json:"methods,omitempty"`
	}
	for _, tt := range []struct {
		name, raw string
		ok        bool
	}{
		{"valid", `{"methods":["GET"]}`, true},
		{"unknown field", `{"method":["GET"]}`, false},
		{"duplicate key", `{"methods":["GET"],"methods":[]}`, false},
		{"nested duplicate", `{"methods":["GET"]} `, true},
		{"trailing data", `{"methods":["GET"]} {"methods":[]}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var v target
			if err := DecodeStrictJSON([]byte(tt.raw), &v); (err == nil) != tt.ok {
				t.Errorf("DecodeStrictJSON(%s) = %v, want ok=%v", tt.raw, err, tt.ok)
			}
		})
	}
}
