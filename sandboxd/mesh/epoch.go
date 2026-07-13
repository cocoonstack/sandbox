package mesh

import (
	"os"
	"strconv"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// loadEpoch reads the persisted gossip epoch, or 0 when the file is absent or
// unreadable — the wall-clock seed then wins, which is the pre-persistence
// behavior.
func loadEpoch(path string) uint64 {
	raw, err := os.ReadFile(path) //nolint:gosec // node-local data-dir path
	if err != nil {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func storeEpoch(path string, epoch uint64) error {
	return utils.WriteFileSync(path, []byte(strconv.FormatUint(epoch, 10)), 0o600)
}
