package engine

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
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
