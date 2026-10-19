package pool

import (
	"encoding/json/v2"
	"testing"
)

func TestBackingDeviceIDPreservesPersistedNumbers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		device uint64
		want   string
	}{
		{"linux-zero", backingDeviceID(uint64(0)), "0"},
		{"linux-high-bit", backingDeviceID(uint64(1) << 63), "9223372036854775808"},
		{"linux-max", backingDeviceID(^uint64(0)), "18446744073709551615"},
		{"darwin-positive", backingDeviceID(int32(16777234)), "16777234"},
		{"darwin-negative", backingDeviceID(int32(-1)), "18446744073709551615"},
		{"darwin-min", backingDeviceID(int32(-2147483648)), "18446744071562067968"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(backingIdentity{Device: tc.device})
			if err != nil {
				t.Fatal(err)
			}
			if want := `{"device":` + tc.want + `,"inode":0,"size":0}`; string(raw) != want {
				t.Fatalf("persisted backing identity = %s, want %s", raw, want)
			}
		})
	}
}
