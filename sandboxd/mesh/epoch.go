package mesh

import (
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/utils"
)

// loadEpoch reads the persisted gossip epoch, or 0 when the file is absent,
// unreadable, or holds an implausible value — the wall-clock seed then wins,
// which is the pre-persistence behavior. The epoch is a UnixNano-derived
// counter that only ever increments by one, so a value above MaxInt64 cannot
// have arisen legitimately; treating it as corrupt keeps a crafted or
// bit-rotted file from seeding a saturated counter that can never advance.
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
