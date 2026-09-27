package pool

import (
	"errors"
	"os"
	"path/filepath"
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
	final := filepath.Join(m.goldensDir(), testKey.Hash())
	if err := m.buildGoldenSteps(t.Context(), testKey, "sbx-gb", "snap", final, imageA); err != nil {
		t.Fatalf("buildGoldenSteps: %v", err)
	}
	stamp, err := os.ReadFile(final + goldenStampSuffix)
	if err != nil || string(stamp) != m.goldenStamp(testKey, false, nil, imageA) {
		t.Fatalf("stamp = %q (%v), want one carrying %s", stamp, err, imageA)
	}
	if string(stamp) == m.goldenStamp(testKey, false, nil, imageB) {
		t.Error("stamp matches a different image id")
	}
}

func TestBuildGoldenAdoptsMatchingStampWithoutBoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 1})
		g := seedGolden(t, m, imageA)

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

func TestImageChangeDropsGoldenAndRebuildsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		seedGolden(t, m, imageA)
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 2
		})

		eng.mu.Lock()
		eng.imageIDs = map[string]string{testKey.Template: imageB}
		eng.mu.Unlock()
		m.checkImagesOnce(t.Context())
		synctest.Wait()
		if infos, _ := m.Info(); infos[0].Golden || infos[0].Warm != 0 {
			t.Fatalf("after the image changed info = %+v, want no golden and no warm VMs", infos[0])
		}
		if removed := eng.removedNames(); len(removed) != 2 {
			t.Fatalf("removed = %v, want both warm VMs of the old image", removed)
		}

		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Golden
		})
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 2
		})
		removedBefore := len(eng.removedNames())
		m.checkImagesOnce(t.Context())
		m.refillOnce(t.Context())
		synctest.Wait()
		if n := eng.coldCount(); n != 1 {
			t.Errorf("cold boots = %d, want one rebuild", n)
		}
		if removed := eng.removedNames(); len(removed) != removedBefore {
			t.Errorf("removed = %v, want no trim on an unchanged image", removed)
		}
		stamp, err := os.ReadFile(filepath.Join(m.goldensDir(), testKey.Hash()) + goldenStampSuffix)
		if err != nil || !strings.Contains(string(stamp), imageB) {
			t.Errorf("rebuilt stamp = %q (%v), want %s", stamp, err, imageB)
		}
	})
}

func TestImageListFailureChangesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		eng := newFakeEngine()
		eng.imageIDs = map[string]string{testKey.Template: imageA}
		m := newTestManager(t, eng, config.PoolSpec{PoolKey: testKey, Warm: 2})
		g := seedGolden(t, m, imageA)
		m.refillOnce(t.Context())
		waitFor(t, func() bool {
			infos, _ := m.Info()
			return infos[0].Warm == 2
		})

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
