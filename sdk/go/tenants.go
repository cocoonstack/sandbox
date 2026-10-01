package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync"
)

// TenantSpec is one tenant for SetTenants and PutTenant; an empty Token keeps an existing tenant's token.
type TenantSpec struct {
	Name      string `json:"name"`
	Token     string `json:"token,omitempty"`
	MaxClaims int    `json:"max_claims,omitzero"`
}

// TenantInfo is one tenant on a node, never its token; Removed marks a tenant gone from the set that still owns claims there.
type TenantInfo struct {
	Name      string `json:"name"`
	MaxClaims int    `json:"max_claims,omitzero"`
	Claims    int    `json:"claims"`
	Removed   bool   `json:"removed,omitzero"`
}

// TenantList is a node's tenant set; two nodes with equal Digests hold the same set.
type TenantList struct {
	Tenants []TenantInfo `json:"tenants"`
	Digest  string       `json:"digest"`
}

// NodeResult is one node's outcome from a cluster-wide call; only SetPoolsCluster sets Info.
type NodeResult struct {
	Addr string
	Info *NodeInfo
	Err  error
}

type tenantsUpdate struct {
	Tenants []TenantSpec `json:"tenants"`
}

type tenantUpdate struct {
	Token     string `json:"token,omitempty"`
	MaxClaims int    `json:"max_claims,omitzero"`
}

// Tenants lists the entry node's tenant set (GET /v1/tenants); requires the operator token.
func (c *Client) Tenants(ctx context.Context) (*TenantList, error) {
	return doJSONPtr[TenantList](ctx, c, http.MethodGet, c.addr, "/v1/tenants", nil, c.apiToken, "tenants")
}

// SetTenants replaces the entry node's whole tenant set (PUT /v1/tenants); a tenant left out stops authenticating, its claims stay.
func (c *Client) SetTenants(ctx context.Context, tenants []TenantSpec) (*TenantList, error) {
	return c.setTenantsAt(ctx, c.addr, tenants)
}

// PutTenant adds or changes one tenant on the entry node (PUT /v1/tenants/{name}).
func (c *Client) PutTenant(ctx context.Context, tenant TenantSpec) error {
	return c.putTenantAt(ctx, c.addr, tenant)
}

// DeleteTenant removes one tenant from the entry node (DELETE /v1/tenants/{name}); an unknown name is a 404 *APIError.
func (c *Client) DeleteTenant(ctx context.Context, name string) error {
	return c.deleteTenantAt(ctx, c.addr, name)
}

// SetTenantsCluster applies the tenant set to the entry node and every peer; retrying the failed nodes is the whole protocol.
func (c *Client) SetTenantsCluster(ctx context.Context, tenants []TenantSpec) ([]NodeResult, error) {
	return c.eachNode(ctx, func(addr string) (*NodeInfo, error) {
		_, err := c.setTenantsAt(ctx, addr, tenants)
		return nil, err
	})
}

// PutTenantCluster adds or changes one tenant on the entry node and every peer.
func (c *Client) PutTenantCluster(ctx context.Context, tenant TenantSpec) ([]NodeResult, error) {
	return c.eachNode(ctx, func(addr string) (*NodeInfo, error) { return nil, c.putTenantAt(ctx, addr, tenant) })
}

// DeleteTenantCluster removes one tenant from the entry node and every peer; a node that answers unknown tenant counts as done.
func (c *Client) DeleteTenantCluster(ctx context.Context, name string) ([]NodeResult, error) {
	return c.eachNode(ctx, func(addr string) (*NodeInfo, error) {
		err := c.deleteTenantAt(ctx, addr, name)
		if apiErr, ok := errors.AsType[*APIError](err); ok && apiErr.Status == http.StatusNotFound && apiErr.Message == "unknown tenant" {
			return nil, nil
		}
		return nil, err
	})
}

func (c *Client) setTenantsAt(ctx context.Context, addr string, tenants []TenantSpec) (*TenantList, error) {
	body, err := encodeBody("tenants", tenantsUpdate{Tenants: tenants})
	if err != nil {
		return nil, err
	}
	return doJSONPtr[TenantList](ctx, c, http.MethodPut, addr, "/v1/tenants", bytes.NewReader(body), c.apiToken, "tenants")
}

func (c *Client) putTenantAt(ctx context.Context, addr string, tenant TenantSpec) error {
	body, err := encodeBody("tenant", tenantUpdate{Token: tenant.Token, MaxClaims: tenant.MaxClaims})
	if err != nil {
		return err
	}
	return doNoContent(ctx, c, http.MethodPut, addr, "/v1/tenants/"+url.PathEscape(tenant.Name), bytes.NewReader(body), c.apiToken, "tenant")
}

func (c *Client) deleteTenantAt(ctx context.Context, addr, name string) error {
	return doNoContent(ctx, c, http.MethodDelete, addr, "/v1/tenants/"+url.PathEscape(name), nil, c.apiToken, "delete tenant")
}

// eachNode runs do on the entry node and every peer at once; a non-nil error means peer discovery failed and only the entry node was reached.
func (c *Client) eachNode(ctx context.Context, do func(addr string) (*NodeInfo, error)) ([]NodeResult, error) {
	peers, peersErr := c.peersOrErr(ctx)
	addrs := slices.Compact(slices.Sorted(slices.Values(append([]string{c.addr}, peers...))))
	results := make([]NodeResult, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Go(func() {
			info, err := do(addr)
			results[i] = NodeResult{Addr: addr, Info: info, Err: err}
		})
	}
	wg.Wait()
	if peersErr != nil {
		return results, fmt.Errorf("discover peers: %w", peersErr)
	}
	return results, nil
}
