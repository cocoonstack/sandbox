package config

import (
	"errors"
	"path/filepath"
	"strings"
)

type ManagedVolumeHibernate struct {
	HookArgv           []string `json:"hook_argv"`
	HookTimeoutSeconds int      `json:"hook_timeout_seconds"`
}

func (h ManagedVolumeHibernate) Validate() error {
	if len(h.HookArgv) == 0 || !filepath.IsAbs(h.HookArgv[0]) || filepath.Clean(h.HookArgv[0]) != h.HookArgv[0] || h.HookTimeoutSeconds < 1 || h.HookTimeoutSeconds > 300 {
		return errors.New("managed_volume_hibernate requires absolute hook_argv and hook_timeout_seconds from 1 through 300")
	}
	for _, arg := range h.HookArgv {
		if strings.ContainsRune(arg, 0) {
			return errors.New("managed_volume_hibernate hook_argv contains NUL")
		}
	}
	return nil
}
