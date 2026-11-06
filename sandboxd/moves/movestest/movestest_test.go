package movestest

import (
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/moves"
)

func TestMemContract(t *testing.T) {
	Run(t, func(*testing.T) moves.Registry { return NewMem() })
}
