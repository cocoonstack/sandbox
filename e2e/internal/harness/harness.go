// Package harness carries the claim prologue shared by the e2e tools.
package harness

import (
	"context"
	"fmt"

	sandbox "github.com/cocoonstack/sandbox/sdk/go"
)

// Claim connects to a node and claims one sandbox; the caller owns Close.
func Claim(ctx context.Context, addr, token, template string, opts ...sandbox.Option) (*sandbox.Client, *sandbox.Sandbox, error) {
	client, err := sandbox.Connect(addr, sandbox.WithAPIToken(token))
	if err != nil {
		return nil, nil, err
	}
	sb, err := client.New(ctx, template, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("claim: %w", err)
	}
	return client, sb, nil
}
