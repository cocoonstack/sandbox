package peer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

const testID = "ck_00000000000000aa"

func TestInertWithoutOwnersOrPuller(t *testing.T) {
	h := NewHealer(nil, nil)
	if err := h.Pull(t.Context(), testID, t.TempDir(), nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Pull error = %v, want store.ErrNotFound", err)
	}
}

func TestPullWritesStaging(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{"peer-a:7777": record()}}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	dst := t.TempDir()
	if err := h.Pull(t.Context(), testID, dst, nil); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(dst, store.ExportDir, "mem")); err != nil || string(got) != "guest-pages" {
		t.Fatalf("staged export = %q, %v; want the peer's bytes", got, err)
	}
}

func TestHealTriesNextOwner(t *testing.T) {
	puller := &fakePuller{
		records:  map[string]map[string]string{"peer-b:7777": record()},
		failAddr: "peer-a:7777",
	}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777", "peer-b:7777"} }, puller)

	if err := h.Pull(t.Context(), testID, t.TempDir(), nil); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(puller.asked) != 2 {
		t.Errorf("asked = %v, want both owners tried in order", puller.asked)
	}
}

func TestHealAllOwnersMissStaysNotFound(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{}}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	if err := h.Pull(t.Context(), testID, t.TempDir(), nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Pull error = %v, want store.ErrNotFound", err)
	}
}

func TestHealNoOwners(t *testing.T) {
	puller := &fakePuller{}
	h := NewHealer(func(string) []string { return nil }, puller)

	if err := h.Pull(t.Context(), testID, t.TempDir(), nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Pull error = %v, want store.ErrNotFound", err)
	}
	if len(puller.asked) != 0 {
		t.Errorf("puller was called with no owners: %v", puller.asked)
	}
}

func TestHealErrorIsReported(t *testing.T) {
	puller := &fakePuller{failAddr: "peer-a:7777"}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	err := h.Pull(t.Context(), testID, t.TempDir(), nil)
	if err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Pull error = %v, want the peer failure surfaced", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("peer exploded")) {
		t.Errorf("error = %v, want the underlying peer failure named", err)
	}
}

func TestPullDoesNotDedupConcurrentCalls(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{"peer-a:7777": record()}}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	dst1, dst2 := t.TempDir(), t.TempDir()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, dst := range []string{dst1, dst2} {
		wg.Go(func() { errs[i] = h.Pull(t.Context(), testID, dst, nil) })
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Pull %d: %v", i, err)
		}
	}
	if len(puller.asked) != 2 {
		t.Errorf("puller called %d times, want 2: Pull must not dedup — that's the caller's job now", len(puller.asked))
	}
	for _, dst := range []string{dst1, dst2} {
		if got, err := os.ReadFile(filepath.Join(dst, store.ExportDir, "mem")); err != nil || string(got) != "guest-pages" {
			t.Errorf("staged export at %s = %q, %v; want every caller's own dir populated", dst, got, err)
		}
	}
}

func TestPullValidateRejectionTriesNextOwner(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{
		"peer-a:7777": record(),
		"peer-b:7777": record(),
	}}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777", "peer-b:7777"} }, puller)

	var calls int
	validate := func(string) error {
		calls++
		if calls == 1 {
			return errors.New("invalid record")
		}
		return nil
	}
	if err := h.Pull(t.Context(), testID, t.TempDir(), validate); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if len(puller.asked) != 2 {
		t.Errorf("asked = %v, want both owners tried after the first failed validation", puller.asked)
	}
}

func TestPullValidateRejectsEveryOwner(t *testing.T) {
	puller := &fakePuller{records: map[string]map[string]string{"peer-a:7777": record()}}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777"} }, puller)

	err := h.Pull(t.Context(), testID, t.TempDir(), func(string) error { return errors.New("bad shape") })
	if err == nil || errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Pull error = %v, want the validation failure surfaced", err)
	}
}

func TestPullBudgetBoundsSlowOwners(t *testing.T) {
	puller := &sleepyPuller{sleep: 300 * time.Millisecond}
	h := NewHealer(func(string) []string { return []string{"peer-a:7777", "peer-b:7777"} }, puller)
	h.budget = 200 * time.Millisecond

	start := time.Now()
	err := h.Pull(t.Context(), testID, t.TempDir(), nil)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Pull took %v, want bounded near the %v budget", elapsed, h.budget)
	}
	if err == nil {
		t.Error("Pull with two owners that never finish in time should fail, not silently succeed")
	}
}

type fakePuller struct {
	mu       sync.Mutex
	records  map[string]map[string]string
	asked    []string
	failAddr string
}

func (p *fakePuller) Pull(_ context.Context, addr, _, dst string) error {
	p.mu.Lock()
	p.asked = append(p.asked, addr)
	p.mu.Unlock()
	if addr == p.failAddr {
		return errors.New("peer exploded")
	}
	files, ok := p.records[addr]
	if !ok {
		return ErrNotFound
	}
	for name, content := range files {
		full := filepath.Join(dst, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
			return err
		}
	}
	return nil
}

type sleepyPuller struct {
	sleep time.Duration
}

func (p *sleepyPuller) Pull(ctx context.Context, _, _, _ string) error {
	select {
	case <-time.After(p.sleep):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func record() map[string]string {
	return map[string]string{
		store.MetaFile:                        `{"id":"` + testID + `"}`,
		filepath.Join(store.ExportDir, "mem"): "guest-pages",
	}
}
