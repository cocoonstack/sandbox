package peer

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/cocoonstack/sandbox/sandboxd/store"
)

const partialSuffix = ".partial"

// SendRound PUTs the named files of dir to a peer as one tar stream, in the given order, sparse files kept sparse.
func SendRound(ctx context.Context, addr, token, path, dir string, names []string) error {
	pr, pw := io.Pipe()
	written := make(chan error, 1)
	go func() {
		err := writeRound(dir, names, pw)
		_ = pw.CloseWithError(err)
		written <- err
	}()
	err := peerCall(ctx, http.MethodPut, URL(addr, path), token, pr, 0, http.StatusOK, http.StatusNoContent)
	_ = pr.Close()
	if writeErr := <-written; writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
		return fmt.Errorf("send round to %s: %w", addr, writeErr)
	}
	if err != nil {
		return fmt.Errorf("send round to %s: %w", addr, err)
	}
	return nil
}

// ReceiveRound writes a SendRound stream into dst, replacing same-named files; a file takes its name only once all of it arrived.
func ReceiveRound(r io.Reader, dst string) error {
	open, err := receiveFiles(tar.NewReader(r), dst)
	if err != nil && open != "" {
		_ = os.Remove(open + partialSuffix)
	}
	return err
}

func writeRound(dir string, names []string, w io.Writer) error {
	tw := tar.NewWriter(w)
	scratch := sync.OnceValue(func() []byte { return make([]byte, store.DigestChunkSize) })
	for _, name := range names {
		path := filepath.Join(dir, name)
		fi, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err = tarFile(path, name, fi, tw, scratch); err != nil {
			return fmt.Errorf("tar %s: %w", name, err)
		}
	}
	if err := tw.WriteHeader(&tar.Header{Name: recordTrailer, Mode: 0o600, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	return tw.Close()
}

// receiveFiles returns the target whose partial file is still on disk.
func receiveFiles(tr *tar.Reader, dst string) (string, error) {
	sizes := map[string]int64{}
	open := ""
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return open, errors.New("receive round: no completion marker (transfer cut short)")
		}
		if err != nil {
			return open, fmt.Errorf("receive round: %w", err)
		}
		if hdr.Name == recordTrailer {
			return open, commitPartial(open)
		}
		target, err := safeJoin(dst, hdr.Name)
		if err != nil {
			return open, err
		}
		if raw, ok := hdr.PAXRecords[paxChunk]; ok && target == open {
			if err = writeChunk(open+partialSuffix, raw, tr, sizes); err != nil {
				return open, err
			}
			continue
		}
		rawSize, ok := hdr.PAXRecords[paxFileSize]
		if !ok {
			return open, fmt.Errorf("receive round: unexpected entry %q", hdr.Name)
		}
		if err = commitPartial(open); err != nil {
			return open, err
		}
		open = target
		if err = os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return open, fmt.Errorf("receive round mkdir %s: %w", filepath.Dir(hdr.Name), err)
		}
		mode := os.FileMode(hdr.Mode).Perm() //nolint:gosec // Perm masks to 0777
		if err = createSparse(target+partialSuffix, rawSize, mode, sizes); err != nil {
			return open, err
		}
	}
}

func commitPartial(target string) error {
	if target == "" {
		return nil
	}
	return os.Rename(target+partialSuffix, target)
}
