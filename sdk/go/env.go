package sandbox

import (
	"bytes"
	"context"
	"net/http"
)

type envBody struct {
	Env map[string]EnvVar `json:"env"`
}

type envPatchBody struct {
	Env map[string]*EnvVar `json:"env"`
}

// Env reads the claim's env with the node API token; a host-only entry comes back with an empty Value.
func (s *Sandbox) Env(ctx context.Context) (map[string]EnvVar, error) {
	resp, err := doJSON[envBody](ctx, s.c, http.MethodGet, s.owner, "/v1/sandboxes/"+s.ID+"/env", nil, s.c.apiToken, "read env")
	if err != nil {
		return nil, err
	}
	return resp.Env, nil
}

// SetEnv replaces the claim's whole env with the node API token; an empty env clears it.
func (s *Sandbox) SetEnv(ctx context.Context, env map[string]EnvVar) error {
	body, err := encodeBody("set env", envBody{Env: env})
	if err != nil {
		return err
	}
	return doNoContent(ctx, s.c, http.MethodPut, s.owner, "/v1/sandboxes/"+s.ID+"/env", bytes.NewReader(body), s.c.apiToken, "set env")
}

// PatchEnv sets each entry of patch, removes each nil one and keeps the rest as stored, host-only values included.
func (s *Sandbox) PatchEnv(ctx context.Context, patch map[string]*EnvVar) error {
	body, err := encodeBody("patch env", envPatchBody{Env: patch})
	if err != nil {
		return err
	}
	return doNoContent(ctx, s.c, http.MethodPatch, s.owner, "/v1/sandboxes/"+s.ID+"/env", bytes.NewReader(body), s.c.apiToken, "patch env")
}
