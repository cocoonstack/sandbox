package main

// Sandbox-lane filecache agent (M1): host-side sync daemon binding one
// claimed sandbox to one NAS workspace under a multi-writer contract.
//
// Data plane: the guest workspace lives on the sandbox's own local
// virtio-blk-backed filesystem (all IO at local latency). This agent moves
// deltas between the guest (over the sandboxd-relayed silkd protocol, via
// the official SDK) and the NAS workspace (host mount).
//
// Coordination: the NAS itself is the rendezvous. Each writer appends
// journal entries under <ws>/.filecache/journal/ and freshens
// <ws>/.filecache/seq; pullers poll seq (O(1) GETATTR) and fetch only the
// files named in unseen journal entries. Conflicts resolve last-writer-wins
// by journal timestamp; the loser's local copy is preserved in-guest as
// <path>.fc-conflict-<writer>-<ts> before the remote version lands.
import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	sbx "github.com/cocoonstack/sandbox/sdk/go"
)

const fcDir = ".filecache"

type entMeta struct {
	Kind   string `json:"kind"` // f | l
	Size   int64  `json:"size"`
	MtimeS int64  `json:"mtime_s"`
	Target string `json:"target,omitempty"` // symlink target
}

type journalEntry struct {
	Writer string             `json:"writer"`
	Seq    uint64             `json:"seq"`
	TsNs   int64              `json:"ts_ns"`
	Puts   map[string]entMeta `json:"puts,omitempty"`
	Dels   []string           `json:"dels,omitempty"`
}

type sbxState struct {
	SandboxID string             `json:"sandbox_id"`
	Token     string             `json:"token"`
	Owner     string             `json:"owner"`
	Seq       uint64             `json:"seq"`
	Applied   map[string]uint64  `json:"applied"`
	Manifest  map[string]entMeta `json:"manifest"` // last state common to guest+NAS
}

type agent struct {
	sb       *sbx.Sandbox
	ws       string // NAS workspace dir (host mount)
	mount    string // guest workspace dir
	writer   string
	stateDir string
	st       *sbxState
}

func cmdSbx(args []string) error {
	fs := flag.NewFlagSet("sbx-attach", flag.ExitOnError)
	node := fs.String("node", "127.0.0.1:7777", "sandboxd address")
	tokenFile := fs.String("token-file", "/root/filecache/api.token", "sandboxd API bearer token file")
	template := fs.String("template", "mindset-ap-southeast-1.cr.volces.com/g0031/sandbox-rt@sha256:0a8fce5f7af7086ce995963fd812e266c62263212eb801b71dc6c6881c195a32", "pool template (image ref)")
	net := fs.String("net", "none", "network lane: none|egress")
	size := fs.String("size", "small", "size tier")
	ttl := fs.Duration("ttl", 4*time.Hour, "sandbox TTL")
	ws := fs.String("workspace", "", "NAS workspace dir (host path, required)")
	mount := fs.String("mount", "/workspace", "guest workspace dir")
	writer := fs.String("writer", hostnameShort(), "writer id")
	pushIv := fs.Duration("push-interval", 10*time.Second, "push cycle interval")
	pullIv := fs.Duration("pull-interval", 5*time.Second, "pull poll interval")
	stateRoot := fs.String("state-root", "/data00/filecache/sbx", "local state root")
	resume := fs.Bool("resume", false, "reattach to the sandbox recorded in state")
	fs.Parse(args)
	if *ws == "" {
		return fmt.Errorf("--workspace required")
	}
	apiTok, err := os.ReadFile(*tokenFile)
	if err != nil {
		return fmt.Errorf("api token: %w", err)
	}
	c, err := sbx.Connect(*node, sbx.WithAPIToken(strings.TrimSpace(string(apiTok))))
	if err != nil {
		return err
	}

	a := &agent{ws: *ws, mount: *mount, writer: *writer,
		stateDir: filepath.Join(*stateRoot, pathHash(*ws), *writer)}
	if err := os.MkdirAll(a.stateDir, 0o755); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *resume {
		if err := a.loadState(); err != nil {
			return fmt.Errorf("resume: %w", err)
		}
		a.sb = c.Attach(a.st.Owner, a.st.SandboxID, a.st.Token)
		log.Printf("resumed sandbox %s (owner %s)", a.st.SandboxID, a.st.Owner)
	} else {
		s, err := c.New(ctx, *template,
			sbx.WithNetwork(sbx.NetShape(*net)), sbx.WithSize(sbx.Size(*size)), sbx.WithTimeout(*ttl))
		if err != nil {
			return fmt.Errorf("claim: %w", err)
		}
		a.sb = s
		a.st = &sbxState{SandboxID: s.ID, Token: s.Token(), Owner: s.Owner(),
			Applied: map[string]uint64{}, Manifest: map[string]entMeta{}}
		log.Printf("claimed sandbox %s (owner %s, deadline %s)", s.ID, s.Owner(), s.Deadline.Format(time.TimeOnly))
		if err := a.bootstrap(ctx); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	}
	if err := a.saveState(); err != nil {
		return err
	}
	fmt.Printf("SANDBOX %s %s %s\n", a.st.SandboxID, a.st.Token, a.st.Owner)

	log.Printf("sync loops: push=%s pull=%s workspace=%s writer=%s", *pushIv, *pullIv, a.ws, a.writer)
	pushT := time.NewTicker(*pushIv)
	pullT := time.NewTicker(*pullIv)
	hbT := time.NewTicker(10 * time.Second)
	defer pushT.Stop()
	defer pullT.Stop()
	defer hbT.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down: final push")
			fctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if err := a.pushCycle(fctx); err != nil {
				log.Printf("final push: %v", err)
			}
			cancel()
			return a.saveState()
		case <-pushT.C:
			if err := a.pushCycle(ctx); err != nil && ctx.Err() == nil {
				log.Printf("push: %v", err)
			}
		case <-pullT.C:
			if err := a.pullCycle(ctx); err != nil && ctx.Err() == nil {
				log.Printf("pull: %v", err)
			}
		case <-hbT.C:
			a.heartbeat()
		}
	}
}

// bootstrap hydrates the guest from the NAS tree and initializes the
// manifest to that common state.
func (a *agent) bootstrap(ctx context.Context) error {
	for _, d := range []string{filepath.Join(a.ws, fcDir, "journal"), filepath.Join(a.ws, fcDir, "writers")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	if _, err := a.sb.Exec(ctx, "/bin/sh", "-c", "/usr/bin/mkdir -p "+a.mount); err != nil {
		return fmt.Errorf("guest mkdir: %w", err)
	}
	nas, err := scanNAS(a.ws)
	if err != nil {
		return err
	}
	if len(nas) > 0 {
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(writeTar(pw, a.ws, nas)) }()
		if err := a.sb.Push(ctx, a.mount, pr); err != nil {
			return fmt.Errorf("hydrate push: %w", err)
		}
	}
	a.st.Manifest = nas
	// The hydrated tree already reflects every writer's committed journal, so
	// mark all of it seen. Recover our own seq high-water from the shared
	// journal too: a restarted writer must not reuse seq numbers, or peers
	// that already recorded the old seq would skip the new entry.
	for w, seq := range maxSeqs(filepath.Join(a.ws, fcDir, "journal")) {
		if w == a.writer {
			a.st.Seq = seq
		} else {
			a.st.Applied[w] = seq
		}
	}
	log.Printf("bootstrap: hydrated %d entries (resume seq=%d)", len(nas), a.st.Seq)
	return nil
}

// pushCycle diffs the guest tree against the manifest and publishes local
// changes to the NAS plus a journal entry.
func (a *agent) pushCycle(ctx context.Context) error {
	listing, err := a.guestListing(ctx)
	if err != nil {
		return err
	}
	var changed []string
	puts := map[string]entMeta{}
	for p, m := range listing {
		old, ok := a.st.Manifest[p]
		if !ok || old.Kind != m.Kind || old.Size != m.Size || old.MtimeS != m.MtimeS || old.Target != m.Target {
			changed = append(changed, p)
			puts[p] = m
		}
	}
	var dels []string
	for p := range a.st.Manifest {
		if _, ok := listing[p]; !ok {
			dels = append(dels, p)
		}
	}
	if len(changed) == 0 && len(dels) == 0 {
		return nil
	}
	start := time.Now()

	// Regular files travel as one tar; symlinks are recreated from metadata.
	var files []string
	for _, p := range changed {
		if puts[p].Kind == "f" {
			files = append(files, p)
		}
	}
	if len(files) > 0 {
		// Conflict guard: a file whose NAS copy diverged from our baseline was
		// written by another writer since we last synced. Preserve their
		// version as a conflict copy (which the journal then propagates to all
		// writers) before we overwrite with ours — last-writer-wins on the
		// canonical name, no silent loss of the loser's bytes.
		confTs := time.Now().Unix()
		for _, p := range files {
			dst := filepath.Join(a.ws, p)
			fi, err := os.Lstat(dst)
			if err != nil {
				continue // not on NAS yet — clean create
			}
			base, known := a.st.Manifest[p]
			if known && base.Kind == "f" && fi.Size() == base.Size && fi.ModTime().Unix() == base.MtimeS {
				continue // NAS matches our baseline — a clean fast-forward
			}
			cp := fmt.Sprintf("%s.fc-conflict-%d", p, confTs)
			if err := os.Rename(dst, filepath.Join(a.ws, cp)); err != nil {
				continue
			}
			cfi, _ := os.Lstat(filepath.Join(a.ws, cp))
			puts[cp] = entMeta{Kind: "f", Size: cfi.Size(), MtimeS: cfi.ModTime().Unix()}
			// Materialize the conflict copy into our own guest too, so the user
			// sees it and it does not read as a deletion (manifest-vs-guest) on
			// the next push. Peers receive it via the journal on their pull.
			if b, err := os.ReadFile(filepath.Join(a.ws, cp)); err == nil {
				a.sb.WriteFile(ctx, filepath.Join(a.mount, cp), b, nil)
			}
			log.Printf("conflict on %s: peer version preserved as %s", p, cp)
		}
		sort.Strings(files)
		if err := a.sb.WriteFile(ctx, "/tmp/.fc-list", []byte(strings.Join(files, "\n")+"\n"), nil); err != nil {
			return err
		}
		if _, err := a.sb.Exec(ctx, "/bin/sh", "-c",
			"/usr/bin/tar --format=pax -cf /tmp/.fc-push.tar -C "+a.mount+" -T /tmp/.fc-list"); err != nil {
			return fmt.Errorf("guest tar: %w", err)
		}
		data, err := a.sb.ReadFile(ctx, "/tmp/.fc-push.tar")
		if err != nil {
			return err
		}
		if err := a.applyTarToNAS(bytes.NewReader(data)); err != nil {
			return err
		}
		a.sb.Exec(ctx, "/bin/sh", "-c", "/bin/rm -f /tmp/.fc-push.tar /tmp/.fc-list")
	}
	for _, p := range changed {
		m := puts[p]
		if m.Kind == "l" {
			dst := filepath.Join(a.ws, p)
			os.MkdirAll(filepath.Dir(dst), 0o755)
			os.Remove(dst)
			if err := os.Symlink(m.Target, dst); err != nil {
				return fmt.Errorf("symlink %s: %w", p, err)
			}
		}
	}
	for _, p := range dels {
		if err := os.Remove(filepath.Join(a.ws, p)); err != nil && !os.IsNotExist(err) {
			log.Printf("nas del %s: %v", p, err)
		}
	}

	a.st.Seq++
	je := journalEntry{Writer: a.writer, Seq: a.st.Seq, TsNs: time.Now().UnixNano(), Puts: puts, Dels: dels}
	if err := a.appendJournal(je); err != nil {
		return err
	}
	for p, m := range puts {
		a.st.Manifest[p] = m
	}
	for _, p := range dels {
		delete(a.st.Manifest, p)
	}
	if err := a.saveState(); err != nil {
		return err
	}
	log.Printf("push seq=%d: %d puts %d dels in %s", a.st.Seq, len(puts), len(dels), time.Since(start).Round(time.Millisecond))
	return nil
}

// pullCycle applies other writers' journal entries into the guest.
func (a *agent) pullCycle(ctx context.Context) error {
	entries, err := a.unseenJournal()
	if err != nil || len(entries) == 0 {
		return err
	}
	start := time.Now()
	// Net effect across entries in timestamp order; later wins per path.
	puts := map[string]entMeta{}
	delsSet := map[string]bool{}
	for _, je := range entries {
		for p, m := range je.Puts {
			puts[p] = m
			delete(delsSet, p)
		}
		for _, p := range je.Dels {
			delsSet[p] = true
			delete(puts, p)
		}
	}

	// Conflict check: a path locally modified since the manifest and about to
	// be overwritten gets its local copy preserved first.
	incoming := make([]string, 0, len(puts)+len(delsSet))
	for p := range puts {
		incoming = append(incoming, p)
	}
	for p := range delsSet {
		incoming = append(incoming, p)
	}
	dirty, err := a.guestStat(ctx, incoming)
	if err != nil {
		return err
	}
	conflictTs := time.Now().Unix()
	for p, cur := range dirty {
		base, seen := a.st.Manifest[p]
		if !seen || cur.Size != base.Size || cur.MtimeS != base.MtimeS {
			cp := fmt.Sprintf("%s.fc-conflict-%s-%d", p, a.writer, conflictTs)
			a.sb.Exec(ctx, "/bin/sh", "-c",
				"/bin/cp -a "+shq(filepath.Join(a.mount, p))+" "+shq(filepath.Join(a.mount, cp)))
			log.Printf("conflict on %s: local copy preserved as %s", p, cp)
		}
	}

	// Apply puts: regular files as one tar built from NAS content.
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	nfiles := 0
	for p, m := range puts {
		switch m.Kind {
		case "f":
			src := filepath.Join(a.ws, p)
			fi, err := os.Lstat(src)
			if err != nil {
				continue // deleted on NAS since; a later journal entry covers it
			}
			hdr := &tar.Header{Name: p, Mode: int64(fi.Mode().Perm()), Size: fi.Size(),
				ModTime: fi.ModTime(), Format: tar.FormatPAX}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			f, err := os.Open(src)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				f.Close()
				return err
			}
			f.Close()
			m.Size, m.MtimeS = fi.Size(), fi.ModTime().Unix()
			puts[p] = m
			nfiles++
		case "l":
			a.sb.Exec(ctx, "/bin/sh", "-c",
				"/bin/ln -sfn "+shq(m.Target)+" "+shq(filepath.Join(a.mount, p)))
		}
	}
	tw.Close()
	if nfiles > 0 {
		if err := a.sb.Push(ctx, a.mount, &tarBuf); err != nil {
			return fmt.Errorf("apply push: %w", err)
		}
	}
	for p := range delsSet {
		a.sb.Remove(ctx, filepath.Join(a.mount, p), false)
	}

	for p, m := range puts {
		a.st.Manifest[p] = m
	}
	for p := range delsSet {
		delete(a.st.Manifest, p)
	}
	for _, je := range entries {
		if je.Seq > a.st.Applied[je.Writer] {
			a.st.Applied[je.Writer] = je.Seq
		}
	}
	if err := a.saveState(); err != nil {
		return err
	}
	log.Printf("pull: applied %d entries (%d puts %d dels) in %s", len(entries), nfiles, len(delsSet), time.Since(start).Round(time.Millisecond))
	return nil
}

// guestListing walks the guest workspace: files and symlinks with metadata.
func (a *agent) guestListing(ctx context.Context) (map[string]entMeta, error) {
	out, err := a.sb.Exec(ctx, "/bin/sh", "-c",
		"cd "+a.mount+" && /usr/bin/find . \\( -type f -o -type l \\) -printf '%y\\t%P\\t%T@\\t%s\\t%l\\n'")
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

// guestStat batch-stats specific guest paths (for conflict detection).
func (a *agent) guestStat(ctx context.Context, paths []string) (map[string]entMeta, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	var sb strings.Builder
	sb.WriteString("cd " + a.mount + " 2>/dev/null || exit 0\n")
	for _, p := range paths {
		sb.WriteString("/usr/bin/stat -c '%n|%Y|%s' " + shq(p) + " 2>/dev/null\n")
	}
	sb.WriteString("exit 0\n") // a missing path's stat must not fail the RPC
	out, err := a.sb.Exec(ctx, "/bin/sh", "-c", sb.String())
	if err != nil {
		return nil, err
	}
	res := map[string]entMeta{}
	for _, line := range strings.Split(out, "\n") {
		parts := strings.SplitN(line, "|", 3)
		if len(parts) != 3 {
			continue
		}
		mt, _ := strconv.ParseInt(parts[1], 10, 64)
		sz, _ := strconv.ParseInt(parts[2], 10, 64)
		res[parts[0]] = entMeta{MtimeS: mt, Size: sz}
	}
	return res, nil
}

// applyTarToNAS extracts a guest-produced tar onto the NAS workspace with
// per-file atomic visibility.
func (a *agent) applyTarToNAS(r io.Reader) error {
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
		dst := filepath.Join(a.ws, rel)
		switch hdr.Typeflag {
		case tar.TypeDir:
			os.MkdirAll(dst, 0o755)
		case tar.TypeSymlink:
			os.MkdirAll(filepath.Dir(dst), 0o755)
			os.Remove(dst)
			if err := os.Symlink(hdr.Linkname, dst); err != nil {
				return err
			}
		case tar.TypeReg:
			os.MkdirAll(filepath.Dir(dst), 0o755)
			tmp := fmt.Sprintf("%s.fc-tmp.%d", dst, os.Getpid())
			f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.FileMode(hdr.Mode).Perm())
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				os.Remove(tmp)
				return err
			}
			if err := f.Close(); err != nil {
				os.Remove(tmp)
				return err
			}
			os.Chtimes(tmp, hdr.ModTime, hdr.ModTime)
			if err := os.Rename(tmp, dst); err != nil {
				os.Remove(tmp)
				return err
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
			if err := tw.WriteHeader(&tar.Header{Name: p, Typeflag: tar.TypeSymlink,
				Linkname: m.Target, Format: tar.FormatPAX}); err != nil {
				return err
			}
		case "f":
			fi, err := os.Lstat(src)
			if err != nil {
				return err
			}
			if err := tw.WriteHeader(&tar.Header{Name: p, Mode: int64(fi.Mode().Perm()),
				Size: fi.Size(), ModTime: fi.ModTime(), Format: tar.FormatPAX}); err != nil {
				return err
			}
			f, err := os.Open(src)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				f.Close()
				return err
			}
			f.Close()
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

func (a *agent) appendJournal(je journalEntry) error {
	b, _ := json.Marshal(je)
	dir := filepath.Join(a.ws, fcDir, "journal")
	name := fmt.Sprintf("%s-%016d.json", a.writer, je.Seq)
	tmp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	seqTmp := filepath.Join(a.ws, fcDir, ".seq.tmp."+a.writer)
	os.WriteFile(seqTmp, []byte(fmt.Sprintf("%d %s\n", je.TsNs, a.writer)), 0o644)
	return os.Rename(seqTmp, filepath.Join(a.ws, fcDir, "seq"))
}

// unseenJournal lists journal entries from other writers newer than applied.
func (a *agent) unseenJournal() ([]journalEntry, error) {
	dir := filepath.Join(a.ws, fcDir, "journal")
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []journalEntry
	for _, e := range ents {
		name := e.Name()
		i := strings.LastIndex(name, "-")
		if i < 0 || !strings.HasSuffix(name, ".json") {
			continue
		}
		w := name[:i]
		seq, err := strconv.ParseUint(strings.TrimSuffix(name[i+1:], ".json"), 10, 64)
		if err != nil || w == a.writer || seq <= a.st.Applied[w] {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
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
		name := e.Name()
		i := strings.LastIndex(name, "-")
		if i < 0 || !strings.HasSuffix(name, ".json") {
			continue
		}
		if seq, err := strconv.ParseUint(strings.TrimSuffix(name[i+1:], ".json"), 10, 64); err == nil {
			if seq > res[name[:i]] {
				res[name[:i]] = seq
			}
		}
	}
	return res
}

func (a *agent) heartbeat() {
	p := filepath.Join(a.ws, fcDir, "writers", a.writer+".json")
	b, _ := json.Marshal(map[string]any{"writer": a.writer, "ts": time.Now().UnixNano(),
		"sandbox": a.st.SandboxID, "node": hostnameShort()})
	tmp := p + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, p)
	}
}

func (a *agent) saveState() error {
	b, _ := json.MarshalIndent(a.st, "", " ")
	tmp := filepath.Join(a.stateDir, "state.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(a.stateDir, "state.json"))
}

func (a *agent) loadState() error {
	b, err := os.ReadFile(filepath.Join(a.stateDir, "state.json"))
	if err != nil {
		return err
	}
	a.st = &sbxState{}
	return json.Unmarshal(b, a.st)
}

// cmdSbxNative claims a sandbox bound to a shared workspace and prints its
// handle. The node's sandboxd (with workspace_root configured) drives the sync
// natively — this process does no syncing, it only claims and exits.
func cmdSbxNative(args []string) error {
	fs := flag.NewFlagSet("sbx-native", flag.ExitOnError)
	node := fs.String("node", "127.0.0.1:7777", "sandboxd address")
	tokenFile := fs.String("token-file", "/root/filecache/api.token", "sandboxd API bearer token file")
	template := fs.String("template", "mindset-ap-southeast-1.cr.volces.com/g0031/sandbox-rt@sha256:0a8fce5f7af7086ce995963fd812e266c62263212eb801b71dc6c6881c195a32", "pool template")
	ws := fs.String("workspace", "", "shared workspace name (required)")
	ttl := fs.Duration("ttl", 4*time.Hour, "sandbox TTL")
	noRedirect := fs.Bool("no-redirect", true, "pin claim to this node")
	fs.Parse(args)
	if *ws == "" {
		return fmt.Errorf("--workspace required")
	}
	apiTok, err := os.ReadFile(*tokenFile)
	if err != nil {
		return fmt.Errorf("api token: %w", err)
	}
	c, err := sbx.Connect(*node, sbx.WithAPIToken(strings.TrimSpace(string(apiTok))))
	if err != nil {
		return err
	}
	opts := []sbx.Option{sbx.WithNetwork(sbx.NetNone), sbx.WithSize(sbx.Small),
		sbx.WithTimeout(*ttl), sbx.WithWorkspace(*ws)}
	if *noRedirect {
		opts = append(opts, sbx.WithNoRedirect())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	s, err := c.New(ctx, *template, opts...)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	fmt.Printf("SANDBOX %s %s %s\n", s.ID, s.Token(), s.Owner())
	return nil
}

// cmdSbxExec runs a command in an already-claimed sandbox (test helper).
func cmdSbxExec(args []string) error {
	fs := flag.NewFlagSet("sbx-exec", flag.ExitOnError)
	node := fs.String("node", "127.0.0.1:7777", "sandboxd address")
	id := fs.String("id", "", "sandbox id")
	token := fs.String("token", "", "sandbox token")
	owner := fs.String("owner", "", "owner address (defaults to --node)")
	fs.Parse(args)
	if *id == "" || *token == "" || fs.NArg() == 0 {
		return fmt.Errorf("usage: sbx-exec --id ID --token TOK [--owner ADDR] -- cmd args...")
	}
	c, err := sbx.Connect(*node)
	if err != nil {
		return err
	}
	own := *owner
	if own == "" {
		own = *node
	}
	s := c.Attach(own, *id, *token)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	code, err := s.Run(ctx, sbx.Cmd{Argv: fs.Args(), Stdout: os.Stdout, Stderr: os.Stderr})
	if err != nil {
		return err
	}
	if code != 0 {
		os.Exit(code)
	}
	return nil
}

func hostnameShort() string {
	h, _ := os.Hostname()
	if i := strings.IndexByte(h, '.'); i > 0 {
		h = h[:i]
	}
	return h
}

func pathHash(p string) string {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(p); i++ {
		h ^= uint64(p[i])
		h *= 1099511628211
	}
	return fmt.Sprintf("%016x", h)
}

func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
