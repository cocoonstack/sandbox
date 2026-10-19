package sandbox

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
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

var managedVolumeName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,19}$`)

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
	if err := validManagedVolumeName(name); err != nil {
		return nil, err
	}
	if create.SizeGiB < 1 || create.SizeGiB > 1024 {
		return nil, fmt.Errorf("volume size_gib must be between 1 and 1024")
	}
	body, err := encodeBody("create volume", create)
	if err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, http.MethodPut, c.addr, volumePath(name), bytes.NewReader(body), c.apiToken)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, apiError("create volume", resp)
	}
	return decodeVolumeResponse("create volume", resp.Body)
}

// GetVolume inspects one managed disk visible to this token.
func (c *Client) GetVolume(ctx context.Context, name string) (*ManagedVolumeInfo, error) {
	if err := validManagedVolumeName(name); err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, http.MethodGet, c.addr, volumePath(name), nil, c.apiToken)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, apiError("get volume", resp)
	}
	return decodeVolumeResponse("get volume", resp.Body)
}

// DeleteVolume deletes exactly id, including when an earlier response was lost.
func (c *Client) DeleteVolume(ctx context.Context, name, id string) error {
	if err := validManagedVolumeName(name); err != nil {
		return err
	}
	if id == "" || strings.ContainsAny(id, "\"\r\n") {
		return fmt.Errorf("volume id must be a nonempty unquoted resource id")
	}
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return apiError("delete volume", resp)
	}
	return nil
}

func validManagedVolumeName(name string) error {
	if !managedVolumeName.MatchString(name) || strings.HasPrefix(name, "cocoon-") {
		return fmt.Errorf("volume name %q must match %s and not start with cocoon-", name, managedVolumeName)
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
	if reply.Volume == nil || reply.Volume.Kind != "managed" || reply.Volume.ID == "" ||
		reply.Volume.Name == "" || reply.Volume.Tenant == "" || reply.Volume.Node == "" ||
		reply.Volume.State == "" || reply.Volume.CreatedAt.IsZero() || reply.Volume.SizeBytes < 1 {
		return nil, fmt.Errorf("decode %s response: missing managed volume fields", verb)
	}
	return reply.Volume, nil
}
