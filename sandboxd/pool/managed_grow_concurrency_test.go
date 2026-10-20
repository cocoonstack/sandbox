package pool

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/engine"
	"github.com/cocoonstack/sandbox/sandboxd/types"
)

type growthBarrierEngine struct {
	*growthTestEngine
	barrierMu       sync.Mutex
	method          string
	occurrence      int
	seen            int
	entered, resume chan struct{}
	unblock         func()
	socket          string
	connections     sync.WaitGroup
}

func (e *growthBarrierEngine) pause(method string) {
	e.barrierMu.Lock()
	block := false
	if method == e.method {
		e.seen++
		block = e.seen == e.occurrence
	}
	e.barrierMu.Unlock()
	if block {
		close(e.entered)
		<-e.resume
	}
}

func (e *growthBarrierEngine) Inspect(ctx context.Context, name string) (types.VMRecord, bool, error) {
	e.pause("Inspect")
	return e.growthTestEngine.Inspect(ctx, name)
}

func (e *growthBarrierEngine) InspectDisk(ctx context.Context, ref engine.DiskRef) (engine.DiskInspection, error) {
	e.pause("InspectDisk")
	return e.growthTestEngine.InspectDisk(ctx, ref)
}

func (e *growthBarrierEngine) GrowDisk(ctx context.Context, req engine.DiskGrowRequest) (engine.DiskGrowResult, error) {
	e.pause("GrowDisk")
	return e.growthTestEngine.GrowDisk(ctx, req)
}

func (e *growthBarrierEngine) ObserveVolume(ctx context.Context, socket, serial, mount string) (engine.GuestVolumeObservation, error) {
	e.pause("ObserveVolume")
	return e.growthTestEngine.ObserveVolume(ctx, socket, serial, mount)
}

func (e *growthBarrierEngine) DialGuestPort(_ context.Context, socket string, port uint16) (net.Conn, error) {
	if socket != e.socket || port != 8080 {
		return nil, fmt.Errorf("dialed %q:%d, want original guest %q:8080", socket, port, e.socket)
	}
	client, server := net.Pipe()
	e.connections.Go(func() {
		defer server.Close()
		_, _ = io.Copy(server, server)
	})
	return client, nil
}

type growthCallResult struct {
	info types.ManagedVolumeInfo
	err  error
}

func (r growthCallResult) String() string {
	return fmt.Sprintf("size=%d generation=%d growth=%+v err=%v", r.info.SizeBytes, r.info.CapacityGeneration, r.info.Growth, r.err)
}

type growthConcurrencyTest struct {
	m       *Manager
	e       *growthBarrierEngine
	sb      *types.Sandbox
	volume  types.ManagedVolumeInfo
	workers sync.WaitGroup
}

func newGrowthConcurrencyTest(t *testing.T, method string, occurrence int) *growthConcurrencyTest {
	t.Helper()
	m, engine, sb, _, volume := newGrowthTest(t)
	e := &growthBarrierEngine{
		growthTestEngine: engine, method: method, occurrence: occurrence,
		entered: make(chan struct{}), resume: make(chan struct{}), socket: sb.VsockSocket,
	}
	e.unblock = sync.OnceFunc(func() { close(e.resume) })
	m.eng = e
	f := &growthConcurrencyTest{m: m, e: e, sb: sb, volume: volume}
	t.Cleanup(func() {
		e.unblock()
		done := make(chan struct{})
		go func() {
			f.workers.Wait()
			e.connections.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("growth test workers did not stop after releasing the engine")
		}
	})
	return f
}

func (f *growthConcurrencyTest) grow(ctx context.Context, req types.VolumeGrow) <-chan growthCallResult {
	done := make(chan growthCallResult, 1)
	f.workers.Go(func() {
		info, err := f.m.GrowVolume(ctx, "data", f.volume.ID, req, "t1")
		done <- growthCallResult{info, err}
	})
	return done
}

func (f *growthConcurrencyTest) call(fn func() error) <-chan error {
	done := make(chan error, 1)
	f.workers.Go(func() { done <- fn() })
	return done
}

func (f *growthConcurrencyTest) waitEntered(t *testing.T, owner <-chan growthCallResult) {
	t.Helper()
	select {
	case <-f.e.entered:
	case result := <-owner:
		t.Fatalf("growth returned before %s barrier: %+v", f.e.method, result)
	case <-time.After(5 * time.Second):
		t.Fatalf("growth did not enter %s", f.e.method)
	}
}

func growthPrompt(t *testing.T, name string, done <-chan error) (error, bool) {
	t.Helper()
	select {
	case err := <-done:
		return err, true
	case <-time.After(time.Second):
		t.Errorf("%s waited for the blocked growth engine", name)
		return nil, false
	}
}

func waitGrowthResult(t *testing.T, done <-chan growthCallResult) growthCallResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("growth did not return after releasing the engine")
		return growthCallResult{}
	}
}

func (f *growthConcurrencyTest) guestAccess(t *testing.T) {
	t.Helper()
	checks := []struct {
		name string
		call func() error
	}{
		{"AgentSocket", func() error {
			socket, err := f.m.AgentSocket(f.sb.ID, f.sb.Token)
			if err == nil && socket != f.e.socket {
				return fmt.Errorf("socket %q, want %q", socket, f.e.socket)
			}
			return err
		}},
		{"operator DialPort", func() error { return f.dial(t.Context(), Cred{Operator: true}) }},
		{"WakeAgentSocket", func() error {
			socket, fenced, release, err := f.m.WakeAgentSocket(t.Context(), f.sb.ID, f.sb.Token)
			if release != nil {
				defer release()
			}
			if err != nil {
				return err
			}
			if socket != f.e.socket {
				return fmt.Errorf("socket %q, want %q", socket, f.e.socket)
			}
			select {
			case <-fenced:
				return errors.New("running guest stream was fenced")
			default:
				return nil
			}
		}},
		{"token DialPort", func() error { return f.dial(t.Context(), Cred{Token: f.sb.Token}) }},
	}
	for _, check := range checks {
		if err, returned := growthPrompt(t, check.name, f.call(check.call)); returned && err != nil {
			t.Errorf("%s refused the running guest during growth: %v", check.name, err)
		}
	}
}

func (f *growthConcurrencyTest) dial(ctx context.Context, cred Cred) error {
	conn, err := f.m.DialPort(ctx, f.sb.ID, cred, 8080)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte("guest")); err != nil {
		return err
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(conn, got); err != nil {
		return err
	}
	if string(got) != "guest" {
		return fmt.Errorf("guest reply %q", got)
	}
	return nil
}

func (f *growthConcurrencyTest) lifecycleRefused(t *testing.T) {
	t.Helper()
	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{"Hibernate", func() error { return f.m.Hibernate(t.Context(), f.sb.ID, Cred{Operator: true}) }},
		{"Release", func() error { return f.m.Release(t.Context(), f.sb.ID, Cred{Operator: true}) }},
	} {
		if err, returned := growthPrompt(t, call.name, f.call(call.fn)); returned {
			if call.name == "Hibernate" && f.m.managedSleepHook == nil && errors.Is(err, ErrVolumeCapture) {
				continue
			}
			var conflict *VolumeError
			if !errors.As(err, &conflict) || conflict.Status != 409 {
				t.Errorf("%s returned %v, want volume status 409", call.name, err)
			}
		}
	}
	if _, ok := f.m.Sandbox(f.sb.ID); !ok {
		t.Error("growth owner lost its claim")
	}
	if _, ok := f.m.managedHold(f.sb.ID); !ok {
		t.Error("growth owner lost its volume hold")
	}
	f.e.mu.Lock()
	defer f.e.mu.Unlock()
	if f.e.stopped[f.sb.VMName] || len(f.e.removes) != 0 {
		t.Errorf("growth owner was stopped or removed: stopped=%t removes=%v", f.e.stopped[f.sb.VMName], f.e.removes)
	}
	for _, op := range f.e.volumeOps {
		if strings.HasPrefix(op, "umount:") {
			t.Errorf("growth owner was unmounted: %s", op)
		}
	}
}

func TestManagedGrowthGuestConnectionsDuringEngineStalls(t *testing.T) {
	for _, barrier := range []struct {
		name, method string
		occurrence   int
	}{
		{"admission VM inspection", "Inspect", 1},
		{"admission disk inspection", "InspectDisk", 1},
		{"admission guest observation", "ObserveVolume", 1},
		{"dispatch disk inspection", "InspectDisk", 2},
		{"disk growth", "GrowDisk", 1},
		{"acknowledged guest observation", "ObserveVolume", 2},
		{"closing disk inspection", "InspectDisk", 3},
	} {
		t.Run(barrier.name, func(t *testing.T) {
			f := newGrowthConcurrencyTest(t, barrier.method, barrier.occurrence)
			owner := f.grow(t.Context(), growRequest())
			f.waitEntered(t, owner)
			f.guestAccess(t)
			f.e.unblock()
			result := waitGrowthResult(t, owner)
			if result.err != nil || result.info.Growth == nil || result.info.Growth.State != "completed" {
				t.Fatalf("growth after releasing engine: %+v", result)
			}
		})
	}
}

func TestManagedGrowthAdmissionAndDispatchRefuseLifecyclePromptly(t *testing.T) {
	for _, scenario := range []struct {
		method string
		sleep  bool
	}{
		{"Inspect", false}, {"Inspect", true}, {"GrowDisk", false}, {"GrowDisk", true},
	} {
		t.Run(fmt.Sprintf("%s managed sleep %t", scenario.method, scenario.sleep), func(t *testing.T) {
			method := scenario.method
			f := newGrowthConcurrencyTest(t, method, 1)
			if scenario.sleep {
				f.m.managedSleepHook = &config.ManagedVolumeHibernate{HookArgv: []string{"/volume-hook"}, HookTimeoutSeconds: 1}
			}
			owner := f.grow(t.Context(), growRequest())
			f.waitEntered(t, owner)
			if method == "Inspect" {
				current, err := f.m.GetVolume("data", "t1")
				if err != nil || current.Growth != nil {
					t.Fatalf("admission barrier must precede durable intent: %+v %v", current, err)
				}
			}
			f.lifecycleRefused(t)
			f.guestAccess(t)
			f.e.unblock()
			_ = waitGrowthResult(t, owner)
		})
	}
}

func TestManagedGrowthCanceledOwnerRetainsExclusionUntilEngineReturns(t *testing.T) {
	for _, method := range []string{"Inspect", "GrowDisk"} {
		t.Run(method, func(t *testing.T) {
			f := newGrowthConcurrencyTest(t, method, 1)
			f.m.managedSleepHook = &config.ManagedVolumeHibernate{HookArgv: []string{"/volume-hook"}, HookTimeoutSeconds: 1}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			owner := f.grow(ctx, growRequest())
			f.waitEntered(t, owner)
			cancel()
			waiterCtx, cancelWaiter := context.WithCancel(t.Context())
			waiter := f.grow(waiterCtx, growRequest())
			cancelWaiter()
			if err, returned := growthPrompt(t, "canceled identical waiter", f.call(func() error {
				return (<-waiter).err
			})); returned && !errors.Is(err, context.Canceled) {
				t.Errorf("canceled waiter returned %v", err)
			}
			for _, changed := range []types.VolumeGrow{
				{SizeGiB: 3, OperationID: growRequest().OperationID},
				{SizeGiB: 2, OperationID: "c51b3d2b-4ccc-41d5-b430-3046340c30b0"},
			} {
				done := f.grow(t.Context(), changed)
				if err, returned := growthPrompt(t, "conflicting growth", f.call(func() error { return (<-done).err })); returned {
					requireVolumeStatus(t, err, 409)
				}
			}
			f.lifecycleRefused(t)
			f.e.unblock()
			result := waitGrowthResult(t, owner)
			if !errors.Is(result.err, context.Canceled) {
				t.Errorf("canceled owner returned %+v", result)
			}
			f.e.growMu.Lock()
			calls := f.e.calls
			f.e.growMu.Unlock()
			current, err := f.m.GetVolume("data", "t1")
			if err != nil {
				t.Fatal(err)
			}
			if method == "Inspect" {
				if calls != 0 || current.Growth != nil {
					t.Errorf("canceled admission published intent or dispatched: calls=%d volume=%+v", calls, current)
				}
			} else if calls != 1 || current.Growth == nil || current.Growth.RuntimeOutcome != "acknowledged" {
				t.Errorf("canceled dispatch lost exact outcome: calls=%d volume=%+v", calls, current)
			} else {
				retry := waitGrowthResult(t, f.grow(t.Context(), growRequest()))
				if retry.err != nil || retry.info.Growth == nil || retry.info.Growth.State != "completed" {
					t.Errorf("exact retry after canceled dispatch: %+v", retry)
				}
				f.e.growMu.Lock()
				defer f.e.growMu.Unlock()
				if f.e.calls != 1 {
					t.Errorf("exact retry repeated the dispatched effect: %d calls", f.e.calls)
				}
			}
		})
	}
}

func TestManagedGrowthIdenticalRequestsJoinOneDispatch(t *testing.T) {
	f := newGrowthConcurrencyTest(t, "Inspect", 1)
	owner := f.grow(t.Context(), growRequest())
	f.waitEntered(t, owner)
	joiner := f.grow(t.Context(), growRequest())
	select {
	case result := <-joiner:
		t.Fatalf("identical request returned before its owner: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	f.e.unblock()
	for _, done := range []<-chan growthCallResult{owner, joiner} {
		result := waitGrowthResult(t, done)
		if result.err != nil || result.info.Growth == nil || result.info.Growth.State != "completed" || result.info.CapacityGeneration != 1 {
			t.Errorf("joined growth: %+v", result)
		}
	}
	f.e.growMu.Lock()
	defer f.e.growMu.Unlock()
	if f.e.calls != 1 {
		t.Errorf("identical requests dispatched %d times", f.e.calls)
	}
}

func TestManagedGrowthCompletedReplayWaitsForDurableCommit(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("write failure %t", fail), func(t *testing.T) {
			f := newGrowthConcurrencyTest(t, "", 0)
			write := f.m.managed.commitFile
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			t.Cleanup(unblock)
			var once sync.Once
			f.m.managed.commitFile = func(path string, raw []byte) error {
				var saved volumeRegistryData
				if err := json.Unmarshal(raw, &saved); err != nil {
					return err
				}
				if v := saved.Volumes["data"]; v.Growth != nil && v.Growth.State == "completed" {
					once.Do(func() { close(entered) })
					<-resume
					if fail {
						return errors.New("completion snapshot failed")
					}
				}
				return write(path, raw)
			}
			owner := f.grow(t.Context(), growRequest())
			select {
			case <-entered:
			case result := <-owner:
				t.Fatalf("growth returned before completion write: %+v", result)
			case <-time.After(5 * time.Second):
				t.Fatal("completion snapshot was not attempted")
			}
			raw, err := os.ReadFile(filepath.Join(f.m.managed.dir, "registry.json"))
			if err != nil {
				t.Fatal(err)
			}
			var persisted volumeRegistryData
			if err := json.Unmarshal(raw, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Volumes["data"].Growth.State == "completed" {
				t.Fatal("completion barrier did not precede durable completion")
			}
			replay := f.grow(t.Context(), growRequest())
			var early *growthCallResult
			select {
			case result := <-replay:
				early = &result
				t.Errorf("exact replay returned before completion was durable: %+v", result)
			case <-time.After(100 * time.Millisecond):
			}
			f.guestAccess(t)
			unblock()
			first := waitGrowthResult(t, owner)
			second := growthCallResult{}
			if early != nil {
				second = *early
			} else {
				second = waitGrowthResult(t, replay)
			}
			if fail {
				if first.err == nil || second.err == nil {
					t.Errorf("failed completion reported success: owner=%+v replay=%+v", first, second)
				}
				requireVolumeStatus(t, f.m.Release(t.Context(), f.sb.ID, Cred{Operator: true}), 409)
			} else {
				for _, result := range []growthCallResult{first, second} {
					if result.err != nil || result.info.Growth == nil || result.info.Growth.State != "completed" || result.info.CapacityGeneration != 1 {
						t.Errorf("durable completed response: %+v", result)
					}
				}
			}
			f.e.growMu.Lock()
			defer f.e.growMu.Unlock()
			if f.e.calls != 1 {
				t.Errorf("completion replay dispatched %d times", f.e.calls)
			}
		})
	}
}
