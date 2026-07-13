package mesh

import (
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// loadEpoch reads the persisted gossip epoch, or 0 when the file is absent,
// unreadable, or above MaxInt64 — a UnixNano-derived counter cannot legitimately
// exceed it, so a corrupt value falls back to the wall-clock seed.
func loadEpoch(path string) uint64 {
	raw, err := os.ReadFile(path) //nolint:gosec // node-local data-dir path
	if err != nil {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n > math.MaxInt64 {
		return 0
	}
	return n
}

func storeEpoch(path string, epoch uint64) error {
	return utils.WriteFileSync(path, []byte(strconv.FormatUint(epoch, 10)), 0o600)
}
