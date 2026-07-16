package sandbox

import (
	"strings"
	"testing"
)

// Checkpoint.New and Template.New pin the network lane and size to the
// snapshot, so WithNetwork/WithSize must fail loud rather than silently no-op.
func TestSnapshotClaimRejectsPinnedAxes(t *testing.T) {
	ck := &Checkpoint{ID: "ck_1", c: &Client{}, addr: "node:1"}
	tpl := &Template{Name: "rt", c: &Client{}, addr: "node:1", net: "none", size: "small"}
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{"checkpoint size", func() error { _, err := ck.New(t.Context(), WithSize(Large)); return err }},
		{"checkpoint network", func() error { _, err := ck.New(t.Context(), WithNetwork(NetEgress)); return err }},
		{"template size", func() error { _, err := tpl.New(t.Context(), WithSize(Large)); return err }},
		{"template network", func() error { _, err := tpl.New(t.Context(), WithNetwork(NetEgress)); return err }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if err == nil || !strings.Contains(err.Error(), "pinned by the snapshot") {
				t.Errorf("got %v, want a pinned-axes rejection", err)
			}
		})
	}
}
