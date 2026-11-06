package engine

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestGuestPortConnReadsDataUntilDone(t *testing.T) {
	silk, gc := newTestGuestPortConn(t, portForwardMax)

	go func() {
		frame, _ := json.Marshal(map[string]string{"type": "data", "data": base64.StdEncoding.EncodeToString([]byte("HTTP/1.1 200 OK"))}, json.Deterministic(true))
		_, _ = silk.Write(append(frame, '\n'))
		_, _ = silk.Write([]byte(`{"type":"done"}` + "\n"))
	}()
	got := make([]byte, 64)
	n, err := gc.Read(got)
	if err != nil || string(got[:n]) != "HTTP/1.1 200 OK" {
		t.Fatalf("read %q, %v", got[:n], err)
	}
	if _, err := gc.Read(got); !errors.Is(err, io.EOF) {
		t.Errorf("after done: %v, want EOF", err)
	}
}

func TestGuestPortConnCutBeforeDoneIsUnexpectedEOF(t *testing.T) {
	for name, trailer := range map[string]string{"between frames": "", "mid frame": `{"type":"da`} {
		t.Run(name, func(t *testing.T) {
			silk, gc := newTestGuestPortConn(t, portForwardMax)

			go func() {
				frame, _ := json.Marshal(map[string]string{"type": "data", "data": base64.StdEncoding.EncodeToString([]byte("partial"))}, json.Deterministic(true))
				_, _ = silk.Write(append(frame, '\n'))
				_, _ = silk.Write([]byte(trailer))
				_ = silk.Close()
			}()
			got := make([]byte, 64)
			if n, err := gc.Read(got); err != nil || string(got[:n]) != "partial" {
				t.Fatalf("read %q, %v", got[:n], err)
			}
			if _, err := gc.Read(got); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("after a cut stream: %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestGuestPortConnWriteWrapsInDataFrame(t *testing.T) {
	silk, gc := newTestGuestPortConn(t, portForwardMax)

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

func TestGuestPortConnCloseWriteSendsDataEnd(t *testing.T) {
	silk, gc := newTestGuestPortConn(t, portForwardMax)

	go func() { _ = gc.CloseWrite() }()

	line, err := bufio.NewReader(silk).ReadString('\n')
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	want, err := wire.EncodeRequest(wire.DataEnd{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if line != string(want)+"\n" {
		t.Errorf("got %q, want %q", line, string(want)+"\n")
	}
}

func TestGuestPortConnRoundTripsFixtures(t *testing.T) {
	const fixtureDir = "../../protocol/wire/fixtures/v1"
	fixture := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatalf("fixture %s: %v", name, err)
		}
		return append(bytes.TrimSpace(raw), '\n')
	}

	silk, gc := newTestGuestPortConn(t, portForwardMax)

	respData, respDone := fixture("resp_data.json"), fixture("resp_done.json")
	go func() {
		_, _ = silk.Write(respData)
		_, _ = silk.Write(respDone)
	}()
	buf := make([]byte, 64)
	n, err := gc.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("fixture data frame read %q, %v", buf[:n], err)
	}
	if _, err := gc.Read(buf); !errors.Is(err, io.EOF) {
		t.Errorf("fixture done frame: %v, want EOF", err)
	}

	silk2, gc2 := newTestGuestPortConn(t, portReadBuf)
	respError := fixture("resp_error.json")
	go func() { _, _ = silk2.Write(respError) }()
	if _, err := gc2.Read(buf); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("fixture error frame: %v, want a typed error", err)
	}

	silk3, gc3 := newTestGuestPortConn(t, portReadBuf)
	go func() { _, _ = gc3.Write([]byte("hello")) }()
	line := make([]byte, 256)
	n, _ = silk3.Read(line)
	if want := fixture("req_data.json"); !bytes.Equal(line[:n], want) {
		t.Errorf("write frame %q, want fixture %q", line[:n], want)
	}
}

func TestPortReadBufHoldsOneBulkFrame(t *testing.T) {
	frame, err := wire.EncodeResponse(wire.DataResp{Data: make([]byte, wire.BulkChunk)})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(frame) + 1; n > portReadBuf {
		t.Errorf("a full bulk data frame is %d bytes with its delimiter, portReadBuf %d cannot take it in one read", n, portReadBuf)
	}
}

func TestGuestPortConnReadsAFirstFrameThatIsAFullBulkFrame(t *testing.T) {
	silk, gc := newTestGuestPortConn(t, portForwardMax)
	payload := patterned(wire.BulkChunk)

	go writeDataFrames(t, silk, payload)

	if got, err := io.ReadAll(gc); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read %d bytes (err %v), want the %d-byte bulk payload", len(got), err, len(payload))
	}
}

func TestGuestPortConnReadsSmallFramesThenBulkFrames(t *testing.T) {
	silk, gc := newTestGuestPortConn(t, portForwardMax)
	chunks := [][]byte{[]byte("HTTP/1.1 200 OK\r\n"), patterned(100), patterned(3000), patterned(wire.BulkChunk), patterned(wire.BulkChunk - 1), []byte("tail")}

	go writeDataFrames(t, silk, chunks...)

	if got, err := io.ReadAll(gc); err != nil || !bytes.Equal(got, bytes.Join(chunks, nil)) {
		t.Fatalf("read %d bytes (err %v), want %d", len(got), err, len(bytes.Join(chunks, nil)))
	}
	if size := gc.r.Size(); size != portReadBuf {
		t.Errorf("reader size %d after a bulk frame, want %d", size, portReadBuf)
	}
}

func TestGuestPortConnRefusesAFrameOverMaxFrame(t *testing.T) {
	for name, trailer := range map[string]string{"unterminated": "", "terminated": "\n"} {
		t.Run(name, func(t *testing.T) {
			silk, gc := newTestGuestPortConn(t, portForwardMax)

			go func() {
				_, _ = silk.Write(append(bytes.Repeat([]byte("a"), wire.MaxFrame+1), trailer...))
				_ = silk.Close()
			}()
			if _, err := gc.Read(make([]byte, 64)); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("oversized frame: %v, want a frame size error", err)
			}
		})
	}
}

func newTestGuestPortConn(t *testing.T, size int) (net.Conn, *guestPortConn) {
	t.Helper()
	client, silk := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = silk.Close() })
	return silk, &guestPortConn{Conn: client, r: bufio.NewReaderSize(client, size)}
}

func writeDataFrames(t *testing.T, silk net.Conn, chunks ...[]byte) {
	t.Helper()
	var stream []byte
	for _, chunk := range chunks {
		frame, err := wire.EncodeResponse(wire.DataResp{Data: chunk})
		if err != nil {
			t.Error(err)
			return
		}
		stream = append(append(stream, frame...), '\n')
	}
	_, _ = silk.Write(append(stream, `{"type":"done"}`+"\n"...))
}

func patterned(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*31 + n)
	}
	return out
}
