package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// The lease enforces single-writer-per-workspace across nodes. It is a file
// on the shared layer created with O_EXCL (atomic on NFSv4). The holder
// refreshes it via tmp+rename; takeover is allowed once the lease file's
// mtime is older than the TTL. The heartbeat runs as a detached re-exec of
// this binary so the CLI can return while the session stays attached.
const leaseName = ".filecache.lease"

type leaseInfo struct {
	Node string    `json:"node"`
	PID  int       `json:"pid"`
	TS   time.Time `json:"ts"`
	ID   string    `json:"id"`
}

type lease struct {
	path     string
	ttl      time.Duration
	stateDir string
	id       string
}

func leasePath(shared string) string { return filepath.Join(shared, leaseName) }

func acquireLease(shared, id, stateDir string, ttl time.Duration) (*lease, error) {
	host, _ := os.Hostname()
	l := &lease{path: leasePath(shared), ttl: ttl, stateDir: stateDir, id: id}
	body, _ := json.Marshal(leaseInfo{Node: host, PID: os.Getpid(), TS: time.Now(), ID: id})

	for attempt := 0; attempt < 2; attempt++ {
		f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			f.Write(body)
			f.Close()
			return l, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		fi, statErr := os.Stat(l.path)
		if statErr != nil {
			continue // raced with a release; retry
		}
		cur, _ := os.ReadFile(l.path)
		var held leaseInfo
		json.Unmarshal(cur, &held)
		switch {
		case held.Node == host && held.ID == id:
			// Orphaned lease of this same workspace session on this node
			// (crash/reboot left it behind). Attach already refuses while the
			// overlay is still mounted, so reclaiming here cannot race a live
			// local session.
			log.Printf("reclaiming own lease (previous session pid %d)", held.PID)
		case time.Since(fi.ModTime()) >= l.ttl:
			log.Printf("stale lease (age %s), taking over from %s", time.Since(fi.ModTime()).Round(time.Second), held.Node)
		default:
			return nil, fmt.Errorf("workspace lease held: %s (age %s < ttl %s)", string(cur), time.Since(fi.ModTime()).Round(time.Second), l.ttl)
		}
		os.Remove(l.path)
	}
	return nil, fmt.Errorf("could not acquire lease at %s", l.path)
}

// startHeartbeat re-execs this binary as a detached refresher and records
// its pid so detach can stop it.
func (l *lease) startHeartbeat() {
	self, err := os.Executable()
	if err != nil {
		log.Printf("heartbeat disabled (no self path): %v", err)
		return
	}
	cmd := exec.Command(self, "_heartbeat", "--path", l.path, "--interval", (l.ttl / 3).String(), "--hb-id", l.id)
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		log.Printf("heartbeat disabled: %v", err)
		return
	}
	os.WriteFile(filepath.Join(l.stateDir, "heartbeat.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
	cmd.Process.Release()
}

func (l *lease) release() { releaseLease(l.path, l.stateDir) }

func releaseLeaseAt(shared, stateDir string) {
	releaseLease(leasePath(shared), stateDir)
}

func releaseLease(path, stateDir string) {
	pidFile := filepath.Join(stateDir, "heartbeat.pid")
	if b, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(string(b)); err == nil && pid > 1 {
			if p, err := os.FindProcess(pid); err == nil {
				p.Kill()
			}
		}
		os.Remove(pidFile)
	}
	os.Remove(path)
}

// runHeartbeat is the hidden refresher subcommand: rewrite the lease file
// via tmp+rename every interval so its mtime stays fresh. Exits when the
// lease file disappears (released or taken over).
func runHeartbeat(path, id string, interval time.Duration) {
	host, _ := os.Hostname()
	for {
		time.Sleep(interval)
		if _, err := os.Stat(path); err != nil {
			return
		}
		body, _ := json.Marshal(leaseInfo{Node: host, PID: os.Getppid(), TS: time.Now(), ID: id})
		tmp := path + ".hb"
		if err := os.WriteFile(tmp, body, 0o644); err != nil {
			continue
		}
		if err := os.Rename(tmp, path); err != nil {
			os.Remove(tmp)
		}
	}
}
