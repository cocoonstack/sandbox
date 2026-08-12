package filecache

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// syncer binds one sandbox (identified by its vsock socket) to one NAS
// workspace as a single writer. It is not safe for concurrent use; the owning
// Session serializes push and pull cycles.
type syncer struct {
	guest  Guest
	sock   string // sandbox vsock socket
	mount  string // guest workspace dir
	ws     string // NAS workspace dir (host mount)
	writer string

	seq      uint64
	applied  map[string]uint64  // per-peer high-water of journal seq applied
	manifest map[string]entMeta // last state common to guest and NAS
}

func newSyncer(g Guest, sock, mount, ws, writer string) *syncer {
	return &syncer{
		guest: g, sock: sock, mount: mount, ws: ws, writer: writer,
		applied: map[string]uint64{}, manifest: map[string]entMeta{},
	}
}

// bootstrap hydrates the guest workspace from the NAS tree and records that
// tree as the shared baseline. A restarted writer recovers its seq high-water
// from the journal so it never reuses a seq number.
func (s *syncer) bootstrap(ctx context.Context) error {
	for _, d := range []string{filepath.Join(s.ws, fcDir, "journal"), filepath.Join(s.ws, fcDir, "writers")} {
		if err := os.MkdirAll(d, 0o755); err != nil { //nolint:gosec // shared NAS tree; peer nodes traverse it
			return err
		}
	}
	if _, err := s.guest.Run(ctx, s.sock, "/bin/sh", "-c", "/usr/bin/mkdir -p "+s.mount); err != nil {
		return fmt.Errorf("guest mkdir: %w", err)
	}
	nas, err := scanNAS(s.ws)
	if err != nil {
		return err
	}
	if len(nas) > 0 {
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(writeTar(pw, s.ws, nas)) }()
		if err := s.guest.PushTar(ctx, s.sock, s.mount, pr); err != nil {
			return fmt.Errorf("hydrate: %w", err)
		}
	}
	s.manifest = nas
	for w, seq := range maxSeqs(filepath.Join(s.ws, fcDir, "journal")) {
		if w == s.writer {
			s.seq = seq
		} else {
			s.applied[w] = seq
		}
	}
	return nil
}

// pushCycle diffs the guest tree against the baseline and publishes local
// changes to the NAS plus a journal entry. Returns the counts for logging.
func (s *syncer) pushCycle(ctx context.Context) (puts, dels int, err error) {
	listing, err := s.guestListing(ctx)
	if err != nil {
		return 0, 0, err
	}
	changed := map[string]entMeta{}
	for p, m := range listing {
		old, ok := s.manifest[p]
		if !ok || old != m {
			changed[p] = m
		}
	}
	var removed []string
	for p := range s.manifest {
		if _, ok := listing[p]; !ok {
			removed = append(removed, p)
		}
	}
	if len(changed) == 0 && len(removed) == 0 {
		return 0, 0, nil
	}

	var files []string
	for p, m := range changed {
		if m.Kind == "f" {
			files = append(files, p)
		}
	}
	if len(files) > 0 {
		if err := s.publishFiles(ctx, files, changed); err != nil {
			return 0, 0, err
		}
	}
	for p, m := range changed {
		if m.Kind == "l" {
			dst := filepath.Join(s.ws, p)
			_ = os.MkdirAll(filepath.Dir(dst), 0o755) //nolint:gosec // shared NAS tree
			_ = os.Remove(dst)
			if err := os.Symlink(m.Target, dst); err != nil {
				return 0, 0, fmt.Errorf("symlink %s: %w", p, err)
			}
		}
	}
	for _, p := range removed {
		if err := os.Remove(filepath.Join(s.ws, p)); err != nil && !os.IsNotExist(err) {
			return 0, 0, fmt.Errorf("nas del %s: %w", p, err)
		}
	}

	s.seq++
	je := journalEntry{Writer: s.writer, Seq: s.seq, TsNs: time.Now().UnixNano(), Puts: changed, Dels: removed}
	if err := s.appendJournal(je); err != nil {
		return 0, 0, err
	}
	for p, m := range changed {
		s.manifest[p] = m
	}
	for _, p := range removed {
		delete(s.manifest, p)
	}
	return len(changed), len(removed), nil
}

// publishFiles tars the changed regular files out of the guest and lands them
// on the NAS with per-file atomic visibility, preserving any peer version that
// diverged from our baseline as a conflict copy.
func (s *syncer) publishFiles(ctx context.Context, files []string, changed map[string]entMeta) error {
	confTs := time.Now().Unix()
	for _, p := range files {
		dst := filepath.Join(s.ws, p)
		fi, err := os.Lstat(dst)
		if err != nil {
			continue // not on NAS yet — clean create
		}
		base, known := s.manifest[p]
		if known && base.Kind == "f" && fi.Size() == base.Size && fi.ModTime().Unix() == base.MtimeS {
			continue // NAS matches our baseline — clean fast-forward
		}
		cp := fmt.Sprintf("%s.fc-conflict-%d", p, confTs)
		if err := os.Rename(dst, filepath.Join(s.ws, cp)); err != nil {
			continue
		}
		cfi, _ := os.Lstat(filepath.Join(s.ws, cp))
		changed[cp] = entMeta{Kind: "f", Size: cfi.Size(), MtimeS: cfi.ModTime().Unix()}
		// Materialize into our own guest so the user sees it and it does not
		// read as a deletion on the next push. Peers get it via the journal.
		if b, err := os.ReadFile(filepath.Join(s.ws, cp)); err == nil { //nolint:gosec // workspace-relative path from our own listing
			_ = s.guest.WriteFile(ctx, s.sock, filepath.Join(s.mount, cp), 0o644, b)
		}
	}
	sort.Strings(files)
	list := strings.Join(files, "\n") + "\n"
	if err := s.guest.WriteFile(ctx, s.sock, "/tmp/.fc-list", 0o644, []byte(list)); err != nil {
		return err
	}
	if _, err := s.guest.Run(ctx, s.sock, "/bin/sh", "-c",
		"/usr/bin/tar --format=pax -cf /tmp/.fc-push.tar -C "+s.mount+" -T /tmp/.fc-list"); err != nil {
		return fmt.Errorf("guest tar: %w", err)
	}
	data, err := s.guest.ReadFile(ctx, s.sock, "/tmp/.fc-push.tar")
	if err != nil {
		return err
	}
	if err := applyTarToNAS(s.ws, bytes.NewReader(data)); err != nil {
		return err
	}
	_, _ = s.guest.Run(ctx, s.sock, "/bin/sh", "-c", "/bin/rm -f /tmp/.fc-push.tar /tmp/.fc-list")
	return nil
}

// pullCycle applies other writers' journal entries into the guest.
func (s *syncer) pullCycle(ctx context.Context) (entries, puts, dels int, err error) {
	pending, err := s.unseenJournal()
	if err != nil || len(pending) == 0 {
		return 0, 0, 0, err
	}
	putSet := map[string]entMeta{}
	delSet := map[string]bool{}
	for _, je := range pending {
		for p, m := range je.Puts {
			putSet[p] = m
			delete(delSet, p)
		}
		for _, p := range je.Dels {
			delSet[p] = true
			delete(putSet, p)
		}
	}

	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	nfiles := 0
	for p, m := range putSet {
		switch m.Kind {
		case "f":
			src := filepath.Join(s.ws, p)
			fi, err := os.Lstat(src)
			if err != nil {
				continue // deleted on NAS since; a later entry covers it
			}
			hdr := &tar.Header{
				Name: p, Mode: int64(fi.Mode().Perm()), Size: fi.Size(),
				ModTime: fi.ModTime(), Format: tar.FormatPAX,
			}
			if werr := tw.WriteHeader(hdr); werr != nil {
				return 0, 0, 0, werr
			}
			f, err := os.Open(src) //nolint:gosec // workspace-relative path from the journal
			if err != nil {
				return 0, 0, 0, err
			}
			if _, cerr := io.Copy(tw, f); cerr != nil {
				_ = f.Close()
				return 0, 0, 0, cerr
			}
			_ = f.Close()
			m.Size, m.MtimeS = fi.Size(), fi.ModTime().Unix()
			putSet[p] = m
			nfiles++
		case "l":
			_, _ = s.guest.Run(ctx, s.sock, "/bin/sh", "-c",
				"/bin/ln -sfn "+shq(m.Target)+" "+shq(filepath.Join(s.mount, p)))
		}
	}
	if err := tw.Close(); err != nil {
		return 0, 0, 0, err
	}
	if nfiles > 0 {
		if err := s.guest.PushTar(ctx, s.sock, s.mount, &tarBuf); err != nil {
			return 0, 0, 0, fmt.Errorf("apply: %w", err)
		}
	}
	for p := range delSet {
		_ = s.guest.Remove(ctx, s.sock, filepath.Join(s.mount, p), false)
	}

	for p, m := range putSet {
		s.manifest[p] = m
	}
	for p := range delSet {
		delete(s.manifest, p)
	}
	for _, je := range pending {
		if je.Seq > s.applied[je.Writer] {
			s.applied[je.Writer] = je.Seq
		}
	}
	return len(pending), nfiles, len(delSet), nil
}

// guestListing walks the guest workspace: files and symlinks with metadata.
func (s *syncer) guestListing(ctx context.Context) (map[string]entMeta, error) {
	out, err := s.guest.Run(ctx, s.sock, "/bin/sh", "-c",
		"cd "+s.mount+" && /usr/bin/find . \\( -type f -o -type l \\) -printf '%y\\t%P\\t%T@\\t%s\\t%l\\n'")
	if err != nil {
		return nil, fmt.Errorf("guest find: %w", err)
	}
	return parseListing(out), nil
}

func parseListing(out string) map[string]entMeta {
	m := map[string]entMeta{}
	for _, line := range strings.Split(out, "\n") {
		parts := strings.SplitN(line, "\t", 5)
		if len(parts) < 4 || parts[1] == "" {
			continue
		}
		p := parts[1]
		if p == fcDir || strings.HasPrefix(p, fcDir+"/") || strings.HasPrefix(p, ".fc-") {
			continue
		}
		mt, _ := strconv.ParseFloat(parts[2], 64)
		sz, _ := strconv.ParseInt(parts[3], 10, 64)
		e := entMeta{Kind: parts[0], Size: sz, MtimeS: int64(mt)}
		if e.Kind == "l" {
			e.Size = 0
			if len(parts) == 5 {
				e.Target = parts[4]
			}
		}
		m[p] = e
	}
	return m
}

// applyTarToNAS extracts a guest-produced tar onto the NAS workspace with
// per-file atomic visibility (temp name + rename).
func applyTarToNAS(ws string, r io.Reader) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rel := filepath.Clean(strings.TrimPrefix(hdr.Name, "./"))
		if rel == "." || strings.HasPrefix(rel, fcDir) {
			continue
		}
		dst := filepath.Join(ws, rel)
		switch hdr.Typeflag {
		case tar.TypeDir:
			_ = os.MkdirAll(dst, 0o755) //nolint:gosec // shared NAS tree
		case tar.TypeSymlink:
			_ = os.MkdirAll(filepath.Dir(dst), 0o755) //nolint:gosec // shared NAS tree
			_ = os.Remove(dst)
			if err := os.Symlink(hdr.Linkname, dst); err != nil {
				return err
			}
		case tar.TypeReg:
			_ = os.MkdirAll(filepath.Dir(dst), 0o755) //nolint:gosec // shared NAS tree
			tmp := fmt.Sprintf("%s.fc-tmp.%d", dst, os.Getpid())
			f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode).Perm()) //nolint:gosec // temp beside its destination inside the workspace
			if err != nil {
				return err
			}
			if _, cerr := io.Copy(f, tr); cerr != nil { //nolint:gosec // guest-produced tar, bounded by workspace
				_ = f.Close()
				_ = os.Remove(tmp)
				return cerr
			}
			if cerr := f.Close(); cerr != nil {
				_ = os.Remove(tmp)
				return cerr
			}
			_ = os.Chtimes(tmp, hdr.ModTime, hdr.ModTime)
			if rerr := os.Rename(tmp, dst); rerr != nil {
				_ = os.Remove(tmp)
				return rerr
			}
		}
	}
}

// writeTar streams the NAS entries as a tar (hydration).
func writeTar(w io.Writer, root string, ents map[string]entMeta) error {
	tw := tar.NewWriter(w)
	paths := make([]string, 0, len(ents))
	for p := range ents {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		m := ents[p]
		src := filepath.Join(root, p)
		switch m.Kind {
		case "l":
			if err := tw.WriteHeader(&tar.Header{
				Name: p, Typeflag: tar.TypeSymlink,
				Linkname: m.Target, Format: tar.FormatPAX,
			}); err != nil {
				return err
			}
		case "f":
			fi, err := os.Lstat(src)
			if err != nil {
				return err
			}
			if werr := tw.WriteHeader(&tar.Header{
				Name: p, Mode: int64(fi.Mode().Perm()),
				Size: fi.Size(), ModTime: fi.ModTime(), Format: tar.FormatPAX,
			}); werr != nil {
				return werr
			}
			f, err := os.Open(src) //nolint:gosec // workspace-relative path from our own listing
			if err != nil {
				return err
			}
			if _, cerr := io.Copy(tw, f); cerr != nil {
				_ = f.Close()
				return cerr
			}
			_ = f.Close()
		}
	}
	return tw.Close()
}

// scanNAS walks the NAS workspace (excluding .filecache): files + symlinks.
func scanNAS(root string) (map[string]entMeta, error) {
	out := map[string]entMeta{}
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			if rel == fcDir {
				return filepath.SkipDir
			}
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			tgt, _ := os.Readlink(p)
			out[rel] = entMeta{Kind: "l", Target: tgt}
		} else if fi.Mode().IsRegular() {
			out[rel] = entMeta{Kind: "f", Size: fi.Size(), MtimeS: fi.ModTime().Unix()}
		}
		return nil
	})
	return out, err
}

func (s *syncer) appendJournal(je journalEntry) error {
	b, _ := json.Marshal(je)
	dir := filepath.Join(s.ws, fcDir, "journal")
	name := fmt.Sprintf("%s-%016d.json", s.writer, je.Seq)
	tmp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil { //nolint:gosec // peer nodes read the journal
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	seqTmp := filepath.Join(s.ws, fcDir, ".seq.tmp."+s.writer)
	if err := os.WriteFile(seqTmp, []byte(fmt.Sprintf("%d %s\n", je.TsNs, s.writer)), 0o644); err != nil { //nolint:gosec // peer nodes read the seq file
		return err
	}
	return os.Rename(seqTmp, filepath.Join(s.ws, fcDir, "seq"))
}

// unseenJournal lists journal entries from other writers newer than applied.
func (s *syncer) unseenJournal() ([]journalEntry, error) {
	dir := filepath.Join(s.ws, fcDir, "journal")
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []journalEntry
	for _, e := range ents {
		w, seq, ok := parseJournalName(e.Name())
		if !ok || w == s.writer || seq <= s.applied[w] {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name())) //nolint:gosec // journal dir entry under the workspace
		if err != nil {
			continue
		}
		var je journalEntry
		if json.Unmarshal(b, &je) == nil {
			out = append(out, je)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TsNs < out[j].TsNs })
	return out, nil
}

func maxSeqs(dir string) map[string]uint64 {
	res := map[string]uint64{}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return res
	}
	for _, e := range ents {
		if w, seq, ok := parseJournalName(e.Name()); ok && seq > res[w] {
			res[w] = seq
		}
	}
	return res
}

func parseJournalName(name string) (writer string, seq uint64, ok bool) {
	i := strings.LastIndex(name, "-")
	if i < 0 || !strings.HasSuffix(name, ".json") {
		return "", 0, false
	}
	seq, err := strconv.ParseUint(strings.TrimSuffix(name[i+1:], ".json"), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return name[:i], seq, true
}

func (s *syncer) heartbeat() {
	p := filepath.Join(s.ws, fcDir, "writers", s.writer+".json")
	b, _ := json.Marshal(map[string]any{"writer": s.writer, "ts": time.Now().UnixNano()})
	tmp := p + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil { //nolint:gosec // peer nodes read writer liveness
		_ = os.Rename(tmp, p)
	}
}

func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
