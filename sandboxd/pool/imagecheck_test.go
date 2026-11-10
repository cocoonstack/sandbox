package pool

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/cocoonstack/sandbox/sandboxd/config"
)

const (
	imageA = "sha256:aaaa"
	imageB = "sha256:bbbb"
)

func TestGoldenStampCarriesImageID(t *testing.T) {
	eng := newFakeEngine()
	eng.imageIDs = map[string]string{testKey.Template: imageA}
	m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
	final := buildGoldenAt(t, m, testKey, imageA)
	stamp, err := os.ReadFile(final + goldenStampSuffix)
	if err != nil || string(stamp) != m.goldenStamp(m.view.Load(), testKey, imageA) {
		t.Fatalf("stamp = %q (%v), want one carrying %s", stamp, err, imageA)
	}
	if string(stamp) == m.goldenStamp(m.view.Load(), testKey, imageB) {
		t.Error("stamp matches a different image id")
	}
}

func TestBuildGoldenAdoptsMatchingStampWithoutBoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		g := seedGolden(t, m, testKey, imageA)

		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Golden && infos[0].Warm == 1
		})
		if n := eng.coldCount(); n != 0 {
			t.Errorf("cold boots = %d, want 0 for a golden whose stamp matches", n)
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if p := m.pools[testKey]; p.goldenDir != g || p.imageID != imageA {
			t.Errorf("golden = %q image = %q, want %q %q", p.goldenDir, p.imageID, g, imageA)
		}
	})
}

func TestImageChangeReplacesTheWarmSetWithoutEmptyingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		seedGolden(t, m, testKey, imageA)
		m.refillOnce(t.Context())
		waitWarm(t, m, 2)
		old := warmNames(m)

		eng.mu.Lock()
		eng.imageIDs = map[string]string{testKey.Template: imageB}
		eng.mu.Unlock()
		m.checkImagesOnce(t.Context())
		synctest.Wait()
		if infos, _ := m.Info(); !infos[0].Golden || infos[0].Warm != 2 || len(eng.removedNames()) != 0 {
			t.Fatalf("after the image changed info = %+v removed %v, want the old golden and its warm VMs still serving", infos[0], eng.removedNames())
		}

		m.refillOnce(t.Context())
		waitFor(t, func() bool { return poolImage(m) == imageB })
		if n := len(warmNames(m)); n == 0 {
			t.Fatal("the warm set emptied while the new golden was installed")
		}
		m.refillOnce(t.Context())
		synctest.Wait()
		m.shrinkOnce(t.Context())
		synctest.Wait()
		now := warmNames(m)
		if len(now) != 2 || slices.ContainsFunc(now, func(name string) bool { return slices.Contains(old, name) }) {
			t.Fatalf("warm %v after the replace, want two VMs of the new golden and none of %v", now, old)
		}
		if removed := removedWarm(eng); len(removed) != 2 || !slices.Contains(removed, old[0]) || !slices.Contains(removed, old[1]) {
			t.Errorf("removed warm VMs = %v, want exactly the old ones %v", removed, old)
		}

		m.checkImagesOnce(t.Context())
		m.refillOnce(t.Context())
		synctest.Wait()
		if n := eng.coldCount(); n != 1 {
			t.Errorf("cold boots = %d, want one rebuild", n)
		}
		stamp, err := os.ReadFile(filepath.Join(m.goldensDir(), testKey.Hash()) + goldenStampSuffix)
		if err != nil || !strings.Contains(string(stamp), imageB) {
			t.Errorf("rebuilt stamp = %q (%v), want %s", stamp, err, imageB)
		}
	})
}

func TestAFailedRebuildKeepsTheOldGoldenServing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		g := seedGolden(t, m, testKey, imageA)
		m.refillOnce(t.Context())
		waitWarm(t, m, 2)

		eng.mu.Lock()
		eng.imageIDs = map[string]string{testKey.Template: imageB}
		eng.runColdErr = errors.New("image pull failed")
		eng.mu.Unlock()
		m.checkImagesOnce(t.Context())
		m.refillOnce(t.Context())
		synctest.Wait()
		m.mu.Lock()
		p := m.pools[testKey]
		golden, image, warm, pending := p.goldenDir, p.imageID, len(p.warm), p.rebuild
		m.mu.Unlock()
		if eng.coldCount() != 1 || golden != g || image != imageA || warm != 2 || !pending || len(removedWarm(eng)) != 0 {
			t.Errorf("after a failed rebuild golden %q image %q warm %d pending %v removed %v, want the old golden serving and the rebuild still pending",
				golden, image, warm, pending, removedWarm(eng))
		}
	})
}

func TestImageListFailureChangesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		g := seedGolden(t, m, testKey, imageA)
		m.refillOnce(t.Context())
		waitWarm(t, m, 2)

		eng.mu.Lock()
		eng.imageErr = errors.New("cocoon image list: exit status 1")
		eng.mu.Unlock()
		m.checkImagesOnce(t.Context())
		synctest.Wait()
		m.mu.Lock()
		p := m.pools[testKey]
		golden, warm := p.goldenDir, len(p.warm)
		m.mu.Unlock()
		if golden != g || warm != 2 || len(eng.removedNames()) != 0 {
			t.Errorf("golden = %q warm = %d removed = %v, want the pool untouched", golden, warm, eng.removedNames())
		}
	})
}

func TestImageCheckSkipsANodeWithoutPools(t *testing.T) {
	eng := newFakeEngine()
	m := newTestManager(t, eng)
	m.checkImagesOnce(t.Context())
	if eng.imageLists != 0 {
		t.Errorf("image lists = %d, want 0 on a node without pools", eng.imageLists)
	}
}

func warmNames(m *Manager) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for _, sb := range m.pools[testKey].warm {
		names = append(names, sb.VMName)
	}
	return names
}

func poolImage(m *Manager) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pools[testKey].imageID
}

func removedWarm(eng *fakeEngine) []string {
	return slices.DeleteFunc(eng.removedNames(), func(name string) bool { return strings.HasPrefix(name, vmPrefix+"gb-") })
}
