package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type syncConfig struct {
	Workers       int
	PreserveMtime bool
}

// replayDelta applies the overlay upper (the exact dirty set: every created,
// modified or deleted entry, deletions encoded as whiteouts) onto the shared
// layer. Deletions are applied first, then directories top-down, then file
// contents in parallel across directories. Each file lands via a temporary
// name plus rename so concurrent readers on other clients never observe a
// partially written file.
func replayDelta(upper, shared string, cfg syncConfig) (int, error) {
	type fileTask struct {
		rel   string
		mode  os.FileMode
		mtime time.Time
	}
	var (
		whiteouts []string
		opaques   []string
		dirs      []string
		files     []fileTask
		symlinks  []string
	)

	err := filepath.WalkDir(upper, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == upper {
			return nil
		}
		rel, _ := filepath.Rel(upper, p)
		fi, err := d.Info()
		if err != nil {
			return err
		}
		st := fi.Sys().(*syscall.Stat_t)
		switch {
		case fi.Mode()&os.ModeCharDevice != 0 && st.Rdev == 0:
			whiteouts = append(whiteouts, rel)
			return nil
		case d.IsDir():
			if hasMetacopy(p) {
				return fmt.Errorf("metacopy xattr on %s: unsupported overlay feature", p)
			}
			if isOpaque(p) {
				opaques = append(opaques, rel)
			}
			dirs = append(dirs, rel)
			return nil
		case fi.Mode()&os.ModeSymlink != 0:
			symlinks = append(symlinks, rel)
			return nil
		case fi.Mode().IsRegular():
			if hasMetacopy(p) {
				return fmt.Errorf("metacopy xattr on %s: unsupported overlay feature", p)
			}
			files = append(files, fileTask{rel, fi.Mode(), fi.ModTime()})
			return nil
		default:
			log.Printf("skipping special file %s (mode %v)", rel, fi.Mode())
			return nil
		}
	})
	if err != nil {
		return 0, err
	}

	// Phase 1: deletions. Whiteout means "this name was removed"; an opaque
	// dir means "the lower dir is fully replaced by the upper dir".
	for _, rel := range append(append([]string{}, whiteouts...), opaques...) {
		if err := os.RemoveAll(filepath.Join(shared, rel)); err != nil {
			return 0, fmt.Errorf("delete %s: %w", rel, err)
		}
	}

	// Phase 2: directories. The NAS serializes mutations within a directory,
	// so parallelize across directories, level by level (parents first).
	byDepth := map[int][]string{}
	maxDepth := 0
	for _, rel := range dirs {
		d := strings.Count(rel, string(filepath.Separator))
		byDepth[d] = append(byDepth[d], rel)
		if d > maxDepth {
			maxDepth = d
		}
	}
	for depth := 0; depth <= maxDepth; depth++ {
		if err := runParallel(cfg.Workers, byDepth[depth], func(rel string) error {
			dst := filepath.Join(shared, rel)
			if fi, err := os.Lstat(dst); err == nil && !fi.IsDir() {
				if err := os.RemoveAll(dst); err != nil {
					return err
				}
			}
			src, _ := os.Lstat(filepath.Join(upper, rel))
			return os.MkdirAll(dst, src.Mode().Perm())
		}); err != nil {
			return 0, err
		}
	}

	// Phase 3: file bodies. Work units are whole directories: files within a
	// directory are written serially (matching the server's per-directory
	// serialization), while up to Workers directories proceed concurrently.
	byDir := map[string][]fileTask{}
	dirOrder := []string{}
	for _, t := range files {
		d := filepath.Dir(t.rel)
		if _, ok := byDir[d]; !ok {
			dirOrder = append(dirOrder, d)
		}
		byDir[d] = append(byDir[d], t)
	}
	if err := runParallel(cfg.Workers, dirOrder, func(dir string) error {
		for _, t := range byDir[dir] {
			if err := copyFileAtomic(filepath.Join(upper, t.rel), filepath.Join(shared, t.rel), t.mode, t.mtime, cfg.PreserveMtime); err != nil {
				return fmt.Errorf("copy %s: %w", t.rel, err)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}

	// Phase 4: symlinks (cheap, serial).
	for _, rel := range symlinks {
		target, err := os.Readlink(filepath.Join(upper, rel))
		if err != nil {
			return 0, err
		}
		dst := filepath.Join(shared, rel)
		os.Remove(dst)
		if err := os.Symlink(target, dst); err != nil {
			return 0, err
		}
	}

	return len(whiteouts) + len(dirs) + len(files) + len(symlinks), nil
}

// runParallel fans items out to a bounded worker pool and returns the first
// error encountered.
func runParallel[T any](workers int, items []T, fn func(T) error) error {
	var (
		wg       sync.WaitGroup
		tasks    = make(chan T, 64)
		firstErr atomic.Value
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				if err := fn(t); err != nil {
					firstErr.CompareAndSwap(nil, err)
				}
			}
		}()
	}
	for _, t := range items {
		if firstErr.Load() != nil {
			break
		}
		tasks <- t
	}
	close(tasks)
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		return e.(error)
	}
	return nil
}

// copyFileAtomic writes src to dst.tmp.<pid> and renames it into place.
// The file mode is applied at create time (the process runs with umask 0),
// avoiding a separate SETATTR round trip per file; mtime restore is one
// optional SETATTR. A directory obstructing the destination is handled on
// rename failure rather than probed upfront (saves one LOOKUP per file).
// Hard links are materialized as independent copies.
func copyFileAtomic(src, dst string, mode os.FileMode, mtime time.Time, preserveMtime bool) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp := fmt.Sprintf("%s.filecache-tmp.%d", dst, os.Getpid())
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if preserveMtime {
		os.Chtimes(tmp, mtime, mtime)
	}
	if err := os.Rename(tmp, dst); err != nil {
		// A directory (or non-empty dir) may occupy the destination name.
		if os.RemoveAll(dst) == nil {
			if err2 := os.Rename(tmp, dst); err2 == nil {
				return nil
			}
		}
		os.Remove(tmp)
		return err
	}
	return nil
}

func isOpaque(dir string) bool {
	return xattrIs(dir, "trusted.overlay.opaque", 'y')
}

func hasMetacopy(p string) bool {
	return xattrIs(p, "trusted.overlay.metacopy", 0)
}

func xattrIs(p, name string, want byte) bool {
	buf := make([]byte, 8)
	n, err := syscall.Getxattr(p, name, buf)
	if err != nil {
		return false
	}
	if want == 0 {
		return true // presence check
	}
	return n >= 1 && buf[0] == want
}
