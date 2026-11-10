package engine

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	balloonTimeout     = 30 * time.Second
	balloonUnsupported = "balloon resize unsupported"
	balloonBadTarget   = "balloon size "
)

var (
	ErrBalloonUnsupported = errors.New("vm has no balloon device")
	ErrBalloonTarget      = errors.New("bad balloon target")
)

// CheckBalloonSupport rejects a cocoon without the vm balloon verb; an older cocoon prints the vm help for it.
func (e *Engine) CheckBalloonSupport(ctx context.Context) error {
	out, err := e.runFor(ctx, balloonTimeout, cmdTermGrace, "vm", "balloon", "--help")
	if err != nil {
		return fmt.Errorf("the balloon verb requires cocoon with vm balloon: %w", err)
	}
	if !bytes.Contains(out, []byte("vm balloon")) {
		return errors.New("the balloon verb requires cocoon with vm balloon")
	}
	return nil
}

// Balloon sets a VM's balloon target, a byte size or a percent of guest memory, and returns without waiting for the guest.
func (e *Engine) Balloon(ctx context.Context, vmName, target string) (types.VMBalloon, error) {
	out, err := e.runFor(ctx, balloonTimeout, cmdTermGrace, "vm", "balloon", argOutput, formatJSON, "--", vmName, target)
	switch {
	case err == nil:
	case strings.Contains(err.Error(), balloonUnsupported):
		return types.VMBalloon{}, ErrBalloonUnsupported
	case strings.Contains(err.Error(), balloonBadTarget):
		return types.VMBalloon{}, fmt.Errorf("%w: %w", ErrBalloonTarget, err)
	default:
		return types.VMBalloon{}, err
	}
	var b types.VMBalloon
	if err := json.Unmarshal(out, &b); err != nil {
		return types.VMBalloon{}, fmt.Errorf("parse vm balloon: %w", err)
	}
	return b, nil
}
