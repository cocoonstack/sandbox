package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// cmdAttach acquires the workspace lease on the shared layer, prepares the
// local upper/work dirs and mounts the overlay at the merged path.
func cmdAttach(root, id, shared, merged string, leaseTTL time.Duration, noResume bool) error {
	if _, err := os.Stat(shared); err != nil {
		return fmt.Errorf("shared dir not accessible: %w", err)
	}
	if merged == "" {
		merged = filepath.Join("/mnt/filecache", id)
	}
	base := filepath.Join(root, id)
	upper := filepath.Join(base, "upper")
	work := filepath.Join(base, "work")

	if st, err := loadState(root, id); err == nil {
		if isMounted(st.Merged) {
			return fmt.Errorf("workspace %s already attached at %s", id, st.Merged)
		}
	}
	if fi, err := os.Stat(upper); err == nil && fi.IsDir() {
		empty, _ := isEmptyDir(upper)
		if !empty {
			if noResume {
				return fmt.Errorf("leftover upper %s holds un-synced delta and --no-resume was given; sync or clear it first", upper)
			}
			log.Printf("resuming previous session: leftover delta in %s will be included in the next barrier", upper)
		}
	}
	for _, d := range []string{upper, work, merged} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}

	lease, err := acquireLease(shared, id, base, leaseTTL)
	if err != nil {
		return fmt.Errorf("lease: %w", err)
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", shared, upper, work)
	if err := syscall.Mount("overlay", merged, "overlay", 0, opts); err != nil {
		lease.release()
		return fmt.Errorf("mount overlay (%s): %w", opts, err)
	}

	st := &wsState{ID: id, Shared: shared, Merged: merged, Upper: upper, Work: work, Attached: time.Now()}
	if err := st.save(root); err != nil {
		syscall.Unmount(merged, 0)
		lease.release()
		return err
	}
	lease.startHeartbeat()
	log.Printf("attached %s: merged=%s upper=%s shared=%s (heartbeat pid %d)", id, merged, upper, shared, os.Getpid())
	fmt.Println(merged)
	return nil
}

// cmdSync replays the current upper delta onto the shared layer without
// detaching. Writes racing with a live sync are picked up by the next
// barrier; the final detach pass is authoritative.
func cmdSync(root, id string, cfg syncConfig, fromDetach bool) error {
	st, err := loadState(root, id)
	if err != nil {
		return err
	}
	if !fromDetach && !isMounted(st.Merged) {
		return fmt.Errorf("workspace %s is not attached", id)
	}
	start := time.Now()
	n, err := replayDelta(st.Upper, st.Shared, cfg)
	if err != nil {
		return err
	}
	st.LastSync = time.Now()
	st.SyncCount++
	if err := st.save(root); err != nil {
		return err
	}
	log.Printf("sync %s: %d entries replayed to %s in %s", id, n, st.Shared, time.Since(start).Round(time.Millisecond))
	return nil
}

// cmdDetach is the barrier: unmount the overlay (quiescing the workspace),
// replay the delta, clear the local upper, release the lease. After it
// returns, other clients opening files on the NAS observe the final state
// through the NAS's native close-to-open semantics.
func cmdDetach(root, id string, cfg syncConfig, noSync bool) error {
	st, err := loadState(root, id)
	if err != nil {
		return err
	}
	if isMounted(st.Merged) {
		if err := syscall.Unmount(st.Merged, 0); err != nil {
			log.Printf("busy unmount, retrying detached: %v", err)
			time.Sleep(time.Second)
			if err := syscall.Unmount(st.Merged, syscall.MNT_DETACH); err != nil {
				return fmt.Errorf("unmount %s: %w", st.Merged, err)
			}
		}
	}
	if !noSync {
		if err := cmdSync(root, id, cfg, true); err != nil {
			return fmt.Errorf("barrier sync failed, delta kept in %s: %w", st.Upper, err)
		}
		if err := os.RemoveAll(st.Upper); err != nil {
			return err
		}
	} else {
		log.Printf("detach --no-sync: delta kept in %s; run attach+detach later to publish it", st.Upper)
	}
	os.RemoveAll(st.Work)
	releaseLeaseAt(st.Shared, filepath.Join(root, id))
	log.Printf("detached %s (synced=%v)", id, !noSync)
	return nil
}

func cmdStatus(root, id string) error {
	ids := []string{}
	if id != "" {
		ids = append(ids, id)
	} else {
		ents, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Println("no workspaces")
				return nil
			}
			return err
		}
		for _, e := range ents {
			if e.IsDir() {
				ids = append(ids, e.Name())
			}
		}
	}
	for _, wid := range ids {
		st, err := loadState(root, wid)
		if err != nil {
			continue
		}
		dirty, files := deltaStats(st.Upper)
		fmt.Printf("%-24s attached=%v merged=%s shared=%s dirty_entries=%d dirty_files=%d syncs=%d last_sync=%s\n",
			wid, isMounted(st.Merged), st.Merged, st.Shared, dirty, files, st.SyncCount, fmtTime(st.LastSync))
	}
	return nil
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "never"
	}
	return t.Format("15:04:05")
}

func isMounted(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return false
	}
	return fi.Sys().(*syscall.Stat_t).Dev != parent.Sys().(*syscall.Stat_t).Dev
}

func isEmptyDir(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	if err != nil && len(names) == 0 {
		return true, nil
	}
	return len(names) == 0, nil
}

// deltaStats counts entries in the upper (the dirty set).
func deltaStats(upper string) (entries, files int) {
	filepath.WalkDir(upper, func(p string, d os.DirEntry, err error) error {
		if err != nil || p == upper {
			return nil
		}
		entries++
		if d.Type().IsRegular() {
			files++
		}
		return nil
	})
	return
}
