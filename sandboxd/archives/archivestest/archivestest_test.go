package archivestest

import (
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/archives"
)

func TestMemContract(t *testing.T) {
	Run(t, func(*testing.T) archives.Registry { return NewMem() })
}
