package sandbox

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type ManagedVolumeInfo struct {
	VolumeInfo
	Holders []VolumeHolder `json:"holders,omitempty"`
}

type ManagedVolumeError struct {
	*APIError
	Code    string
	Volume  string
	Holders []VolumeHolder
}

func (e *ManagedVolumeError) Unwrap() error { return e.APIError }

// VolumeCreate requests a raw managed disk owned by Tenant. A tenant token may leave Tenant empty.
type VolumeCreate struct {
	SizeGiB int64  `json:"size_gib"`
	Tenant  string `json:"tenant,omitempty"`
}

type volumeResponse struct {
	Volume *ManagedVolumeInfo `json:"volume"`
}

// CreateVolume creates a managed disk or returns the same disk for an identical retry.
func (c *Client) CreateVolume(ctx context.Context, name string, create VolumeCreate) (*ManagedVolumeInfo, error) {
	body, err := encodeBody("create volume", create)
	if err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, http.MethodPut, c.addr, volumePath(name), bytes.NewReader(body), c.apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, apiError("create volume", resp)
	}
	return decodeVolumeResponse("create volume", resp.Body)
}

// GetVolume inspects one managed disk visible to this token.
func (c *Client) GetVolume(ctx context.Context, name string) (*ManagedVolumeInfo, error) {
	resp, err := c.roundTrip(ctx, http.MethodGet, c.addr, volumePath(name), nil, c.apiToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("get volume", resp)
	}
	return decodeVolumeResponse("get volume", resp.Body)
}

// GrowVolume grows an unheld managed disk to sizeGiB; repeating the current size is a no-op.
func (c *Client) GrowVolume(ctx context.Context, name, id string, sizeGiB int64) (*ManagedVolumeInfo, error) {
	body, err := encodeBody("grow volume", struct {
		SizeGiB int64 `json:"size_gib"`
	}{sizeGiB})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.requestURL(c.addr, volumePath(name)+"/grow"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", `"`+id+`"`)
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("grow volume", resp)
	}
	return decodeVolumeResponse("grow volume", resp.Body)
}

// DeleteVolume deletes exactly id, including when an earlier response was lost.
func (c *Client) DeleteVolume(ctx context.Context, name, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.requestURL(c.addr, volumePath(name)), nil)
	if err != nil {
		return err
	}
	req.Header.Set("If-Match", `"`+id+`"`)
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return apiError("delete volume", resp)
	}
	return nil
}

func volumePath(name string) string {
	return "/v1/volumes/" + url.PathEscape(name)
}

func decodeVolumeResponse(verb string, body io.Reader) (*ManagedVolumeInfo, error) {
	var reply volumeResponse
	if err := json.UnmarshalRead(body, &reply); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", verb, err)
	}
	if reply.Volume == nil {
		return nil, fmt.Errorf("decode %s response: missing volume", verb)
	}
	return reply.Volume, nil
}
