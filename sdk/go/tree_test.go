package sandbox

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

func TestPushPullRoundTrip(t *testing.T) {
	sb := fakeSandbox(t)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "t.txt", Mode: 0o600, Size: 10}); err != nil {
		t.Fatalf("tar header: %v", err)
	}
	if _, err := tw.Write([]byte("tar-marker")); err != nil {
		t.Fatalf("tar body: %v", err)
	}
	_ = tw.Close()

	if err := sb.Push(t.Context(), "/proj", bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got, err := sb.ReadFile(t.Context(), "/proj/t.txt")
	if err != nil || string(got) != "tar-marker" {
		t.Fatalf("pushed file: %q, %v", got, err)
	}

	var pulled bytes.Buffer
	if err := sb.Pull(t.Context(), "/proj", &pulled); err != nil {
		t.Fatalf("Pull: %v", err)
	}
	tr := tar.NewReader(&pulled)
	found := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read pulled tar: %v", err)
		}
		if hdr.Typeflag == tar.TypeReg && hdr.Name == "proj/t.txt" {
			body, _ := io.ReadAll(tr)
			if string(body) != "tar-marker" {
				t.Errorf("pulled body %q", body)
			}
			found = true
		}
	}
	if !found {
		t.Error("pulled tar missing proj/t.txt")
	}
}
