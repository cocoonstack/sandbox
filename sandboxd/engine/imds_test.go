package engine

import (
	"errors"
	"testing"

	"github.com/cocoonstack/sandbox/protocol/wire"
)

func TestWriteInstanceMetadataReplacesTheGuestDocument(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	doc := []byte(`{"region":"local"}`)
	if err := New("cocoon", nil, nil, false, false, "").WriteInstanceMetadata(t.Context(), path, doc); err != nil {
		t.Fatalf("WriteInstanceMetadata: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.writePath != instanceMetadataPath || fake.writeMode != 0o600 || string(fake.writeData) != string(doc) {
		t.Errorf("wrote %s mode %o data %s, want %s mode 600 data %s", fake.writePath, fake.writeMode, fake.writeData, instanceMetadataPath, doc)
	}
}

func TestWriteInstanceMetadataReportsAGuestWithoutAResponder(t *testing.T) {
	path := sockPath(t)
	fake := serveFakeSilkd(t, path)
	fake.writeErr, fake.writeKind = "create: No such file or directory", wire.KindNotFound
	e := New("cocoon", nil, nil, false, false, "")
	if err := e.WriteInstanceMetadata(t.Context(), path, []byte("{}")); !errors.Is(err, ErrNoInstanceMetadata) {
		t.Errorf("got %v, want ErrNoInstanceMetadata", err)
	}
	fake.mu.Lock()
	fake.writeKind = wire.KindInternal
	fake.mu.Unlock()
	if err := e.WriteInstanceMetadata(t.Context(), path, []byte("{}")); err == nil || errors.Is(err, ErrNoInstanceMetadata) {
		t.Errorf("internal error: got %v, want a plain failure", err)
	}
}
