// filecache: node-local write-back cache for CTO-consistent NAS workspaces.
//
// Semantics: session-granular close-to-open. A lease on the shared layer
// enforces a single writer per workspace across nodes. While attached, all
// mutations land on a local NVMe overlay upper (local latency). A barrier
// (sync/detach) replays the upper delta onto the shared layer; after the
// barrier returns, any other client opening the files observes the final
// state through the NAS's native CTO semantics.
//
// Crash window: everything since the last barrier stays in the local upper
// (persistent on disk); `attach` on the same node resumes it. Losing the
// node loses at most the un-synced delta (RPO = last barrier).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const stateRootDefault = "/data00/filecache"

type wsState struct {
	ID        string    `json:"id"`
	Shared    string    `json:"shared"` // workspace dir on the NAS mount
	Merged    string    `json:"merged"` // overlay mountpoint presented to apps
	Upper     string    `json:"upper"`
	Work      string    `json:"work"`
	Attached  time.Time `json:"attached"`
	LastSync  time.Time `json:"last_sync,omitempty"`
	SyncCount int       `json:"sync_count"`
}

func statePath(root, id string) string { return filepath.Join(root, id, "state.json") }

func loadState(root, id string) (*wsState, error) {
	b, err := os.ReadFile(statePath(root, id))
	if err != nil {
		return nil, err
	}
	st := &wsState{}
	if err := json.Unmarshal(b, st); err != nil {
		return nil, fmt.Errorf("corrupt state for %s: %w", id, err)
	}
	return st, nil
}

func (st *wsState) save(root string) error {
	b, _ := json.MarshalIndent(st, "", "  ")
	tmp := statePath(root, st.ID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, statePath(root, st.ID))
}

func usage() {
	fmt.Fprintf(os.Stderr, `filecache - local write-back cache layer for NAS workspaces (session-granular CTO)

Usage:
  filecache attach  --id ID --shared DIR [--merged DIR] [--state-root DIR] [--no-resume]
  filecache sync    --id ID [--state-root DIR]            best-effort live sync, keeps session attached
  filecache detach  --id ID [--state-root DIR] [--no-sync]  barrier: umount, replay delta, release lease
  filecache status  [--id ID] [--state-root DIR]
  filecache version
`)
	os.Exit(2)
}

func main() {
	syscall.Umask(0) // file modes are applied verbatim at create time (one less SETATTR per file)
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	if cmd == "sbx-attach" || cmd == "sbx-exec" || cmd == "sbx-native" {
		var err error
		switch cmd {
		case "sbx-attach":
			err = cmdSbx(os.Args[2:])
		case "sbx-exec":
			err = cmdSbxExec(os.Args[2:])
		case "sbx-native":
			err = cmdSbxNative(os.Args[2:])
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "filecache %s: %v\n", cmd, err)
			os.Exit(1)
		}
		return
	}
	if cmd == "_heartbeat" {
		hb := flag.NewFlagSet("_heartbeat", flag.ExitOnError)
		path := hb.String("path", "", "")
		interval := hb.Duration("interval", 30*time.Second, "")
		hbID := hb.String("hb-id", "", "")
		hb.Parse(os.Args[2:])
		runHeartbeat(*path, *hbID, *interval)
		return
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	id := fs.String("id", "", "workspace id (unique per workspace)")
	shared := fs.String("shared", "", "workspace directory on the shared NAS mount")
	merged := fs.String("merged", "", "overlay mountpoint (default /mnt/filecache/<id>)")
	stateRoot := fs.String("state-root", stateRootDefault, "local root for upper/work/state")
	noSync := fs.Bool("no-sync", false, "detach without final barrier (discards nothing; delta stays local)")
	noResume := fs.Bool("no-resume", false, "refuse to reuse a leftover upper from a previous session")
	workers := fs.Int("workers", 20, "parallel sync workers (cross-directory)")
	preserveMtime := fs.Bool("preserve-mtime", true, "restore file mtimes on the shared layer during sync")
	leaseTTL := fs.Duration("lease-ttl", 90*time.Second, "lease expiry; stale leases may be taken over")
	fs.Parse(os.Args[2:])

	cfg := syncConfig{Workers: *workers, PreserveMtime: *preserveMtime}

	var err error
	switch cmd {
	case "attach":
		if *id == "" || *shared == "" {
			usage()
		}
		err = cmdAttach(*stateRoot, *id, *shared, *merged, *leaseTTL, *noResume)
	case "sync":
		if *id == "" {
			usage()
		}
		err = cmdSync(*stateRoot, *id, cfg, false)
	case "detach":
		if *id == "" {
			usage()
		}
		err = cmdDetach(*stateRoot, *id, cfg, *noSync)
	case "status":
		err = cmdStatus(*stateRoot, *id)
	case "version":
		fmt.Println("filecache 0.1.0")
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "filecache %s: %v\n", cmd, err)
		os.Exit(1)
	}
}
