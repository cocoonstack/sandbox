package dir

import (
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestDirBackendContract(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}
