// Package tenants holds the tenant set a node authenticates against behind one interface.
package tenants

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const (
	// Unknown means a cached source has not seen the name yet; it is never a removal.
	Unknown Presence = iota
	Present
	Absent
)

var (
	// ErrUnknown answers a token or name no tenant holds.
	ErrUnknown = errors.New("unknown tenant")
	// ErrUnavailable answers a lookup or write the backend cannot serve now; a retry may succeed.
	ErrUnavailable = errors.New("tenant store down")
	// ErrInvalid answers a record the set refuses: a bad field, a reused token, or a new tenant without one.
	ErrInvalid = errors.New("invalid tenant")
)

// TenantSpec is one tenant as the tenant API takes it: its bearer token, its live-claim quota and its egress class.
type TenantSpec struct {
	Name        string `json:"name"`
	Token       string `json:"token"`
	MaxClaims   int    `json:"max_claims,omitzero"`
	EgressClass string `json:"egress_class,omitempty"`
}

// TenantRecord stores a tenant's identity, quota and egress class without its plaintext token.
type TenantRecord struct {
	Name        string `json:"name"`
	TokenSHA256 string `json:"token_sha256"`
	MaxClaims   int    `json:"max_claims,omitzero"`
	EgressClass string `json:"egress_class,omitempty"`
}

// Presence is what a memory-only lookup knows about a name.
type Presence int

// Change names the tenants a write added, changed and removed, for the audit log.
type Change struct {
	Added   []string
	Changed []string
	Removed []string
}

// Empty reports whether the write changed nothing.
func (c Change) Empty() bool {
	return c.Added == nil && c.Changed == nil && c.Removed == nil
}

// Source is one tenant set backend.
type Source interface {
	// Resolve maps a bearer token's SHA-256 to its tenant's shared, read-only record; a cached source may query its backend on a miss.
	Resolve(ctx context.Context, sum [sha256.Size]byte) (*TenantRecord, error)
	// Peek answers from memory only, so a caller holding a lock never waits on the backend.
	Peek(name string) (TenantRecord, Presence)
	// Lookup answers whether name is in the set, asking the backend when a cache has not seen it.
	Lookup(ctx context.Context, name string) (TenantRecord, bool, error)
	// List returns up to limit tenants named after the cursor, in name order, without token hashes.
	List(ctx context.Context, after string, limit int) ([]TenantRecord, error)
	// Put adds or changes one tenant; an empty TokenSHA256 keeps the stored token.
	Put(ctx context.Context, r TenantRecord) (Change, error)
	// Delete removes one tenant; ErrUnknown when it is not in the set.
	Delete(ctx context.Context, name string) (Change, error)
	// Replace makes the set exactly records; an entry without TokenSHA256 keeps the stored token.
	Replace(ctx context.Context, records []TenantRecord) (Change, error)
	// Classes counts the tenants that name each egress class.
	Classes(ctx context.Context) (map[string]int, error)
	// Records returns the whole set for the cluster digest; a shared source returns nil, as every node reads the same rows.
	Records() []TenantRecord
	// Digest fingerprints a node-local set so two nodes can compare theirs; a shared source returns "".
	Digest() string
	Close() error
}

// Validate checks one record's fields; an empty TokenSHA256 passes, since writes may keep the stored token.
func Validate(r TenantRecord, rootSum string) error {
	switch {
	case !types.NameRe.MatchString(r.Name):
		return fmt.Errorf("%w: tenant name %q must match %s", ErrInvalid, r.Name, types.NameRe)
	case r.MaxClaims < 0:
		return fmt.Errorf("%w: tenant %q max_claims must not be negative, got %d", ErrInvalid, r.Name, r.MaxClaims)
	case r.TokenSHA256 == "":
		return nil
	case r.TokenSHA256 == rootSum:
		return fmt.Errorf("%w: tenant %q token must differ from api_token", ErrInvalid, r.Name)
	}
	if sum, err := hex.DecodeString(r.TokenSHA256); err != nil || len(sum) != sha256.Size {
		return fmt.Errorf("%w: tenant %q token_sha256 must be %d hex bytes", ErrInvalid, r.Name, sha256.Size)
	}
	return nil
}

// Diff names the tenants next adds, changes and removes against cur, each list in name order.
func Diff(cur, next map[string]TenantRecord) Change {
	var c Change
	for _, name := range slices.Sorted(maps.Keys(next)) {
		switch old, ok := cur[name]; {
		case !ok:
			c.Added = append(c.Added, name)
		case old != next[name]:
			c.Changed = append(c.Changed, name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cur)) {
		if _, ok := next[name]; !ok {
			c.Removed = append(c.Removed, name)
		}
	}
	return c
}

func KeepTokens(cur map[string]TenantRecord, records []TenantRecord) (map[string]TenantRecord, error) {
	next := make(map[string]TenantRecord, len(records))
	for _, r := range records {
		if _, dup := next[r.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate tenant name %q", ErrInvalid, r.Name)
		}
		if r.TokenSHA256 == "" {
			kept, ok := cur[r.Name]
			if !ok {
				return nil, fmt.Errorf("%w: tenant %q needs a token", ErrInvalid, r.Name)
			}
			r.TokenSHA256 = kept.TokenSHA256
		}
		next[r.Name] = r
	}
	return next, nil
}

// TokenSHA256 is the hex SHA-256 a tenant token is kept as outside config.json.
func TokenSHA256(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
