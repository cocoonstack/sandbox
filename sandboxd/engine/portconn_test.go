package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestGuestPortConnFramesBothWays(t *testing.T) {
	client, silk := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = silk.Close() })
	gc := newGuestPortConn(client)

	// Read: a silkd data frame decodes to raw bytes; done maps to EOF.
	go func() {
		frame, _ := json.Marshal(map[string]string{"type": "data", "data": base64.StdEncoding.EncodeToString([]byte("HTTP/1.1 200 OK"))})
		_, _ = silk.Write(append(frame, '\n'))
		_, _ = silk.Write([]byte(`{"type":"done"}` + "\n"))
	}()
	got := make([]byte, 64)
	n, err := gc.Read(got)
	if err != nil || string(got[:n]) != "HTTP/1.1 200 OK" {
		t.Fatalf("read %q, %v", got[:n], err)
	}
	if _, err := gc.Read(got); err != io.EOF {
		t.Errorf("after done: %v, want EOF", err)
	}
}

func TestGuestPortConnWriteWrapsInDataFrame(t *testing.T) {
	client, silk := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = silk.Close() })
	gc := newGuestPortConn(client)

	go func() { _, _ = gc.Write([]byte("GET / HTTP/1.1\r\n")) }()

	line := make([]byte, 256)
	n, _ := silk.Read(line)
	var frame struct {
		V    int    `json:"v"`
		Op   string `json:"op"`
		Data []byte `json:"data"`
	}
	if err := json.Unmarshal(line[:n], &frame); err != nil {
		t.Fatalf("frame: %v (%q)", err, line[:n])
	}
	if frame.V != 1 || frame.Op != "data" || string(frame.Data) != "GET / HTTP/1.1\r\n" {
		t.Errorf("frame %+v", frame)
	}
}

// TestGuestPortConnRoundTripsFixtures pins the engine's hand-rolled framing
// to the shared protocol corpus: portconn is not built on sdk/go/silkd, so
// without this test it could drift from the frames both real peers agree on.
func TestGuestPortConnRoundTripsFixtures(t *testing.T) {
	const fixtureDir = "../../protocol/fixtures/v1"
	fixture := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("fixture %s: %v", name, err)
		}
		return append(bytes.TrimSpace(raw), '\n')
	}

	client, silk := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = silk.Close() })
	gc := newGuestPortConn(client)

	// Read path: the corpus data frame decodes to its payload, done is EOF.
	go func() {
		_, _ = silk.Write(fixture("resp_data.json"))
		_, _ = silk.Write(fixture("resp_done.json"))
	}()
	buf := make([]byte, 64)
	n, err := gc.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("fixture data frame read %q, %v", buf[:n], err)
	}
	if _, err := gc.Read(buf); err != io.EOF {
		t.Errorf("fixture done frame: %v, want EOF", err)
	}

	// Error frames surface their type, not a silent close.
	client2, silk2 := net.Pipe()
	t.Cleanup(func() { _ = client2.Close(); _ = silk2.Close() })
	gc2 := newGuestPortConn(client2)
	go func() { _, _ = silk2.Write(fixture("resp_error.json")) }()
	if _, err := gc2.Read(buf); err == nil || err == io.EOF {
		t.Errorf("fixture error frame: %v, want a typed error", err)
	}

	// Write path: the emitted frame is byte-identical to the corpus request.
	client3, silk3 := net.Pipe()
	t.Cleanup(func() { _ = client3.Close(); _ = silk3.Close() })
	gc3 := newGuestPortConn(client3)
	go func() { _, _ = gc3.Write([]byte("hello")) }()
	line := make([]byte, 256)
	n, _ = silk3.Read(line)
	if want := fixture("req_data.json"); !bytes.Equal(line[:n], want) {
		t.Errorf("write frame %q, want fixture %q", line[:n], want)
	}
}
