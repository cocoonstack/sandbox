package egress

import "testing"

func TestSecretSpecValidate(t *testing.T) {
	tests := []struct {
		name string
		spec SecretSpec
		ok   bool
	}{
		{"env", SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"}, true},
		{"no name", SecretSpec{Header: "Authorization", ValueEnv: "GH_TOKEN"}, false},
		{"no header", SecretSpec{Name: "gh", ValueEnv: "GH_TOKEN"}, false},
		{"invalid header", SecretSpec{Name: "gh", Header: "Bad Header:", ValueEnv: "GH_TOKEN"}, false},
		{"no value_env", SecretSpec{Name: "gh", Header: "Authorization"}, false},
		{"inline value", SecretSpec{Name: "gh", Header: "Authorization", Value: "tok"}, false},
		{"value and value_env", SecretSpec{Name: "gh", Header: "Authorization", Value: "tok", ValueEnv: "GH_TOKEN"}, false},
		{"host header", SecretSpec{Name: "gh", Header: "Host", ValueEnv: "GH_TOKEN"}, false},
		{"content-length header", SecretSpec{Name: "gh", Header: "Content-Length", ValueEnv: "GH_TOKEN"}, false},
		{"transfer-encoding header", SecretSpec{Name: "gh", Header: "Transfer-Encoding", ValueEnv: "GH_TOKEN"}, false},
		{"connection header", SecretSpec{Name: "gh", Header: "connection", ValueEnv: "GH_TOKEN"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.spec.Validate(); (err == nil) != tt.ok {
				t.Errorf("Validate() = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestSecretStoreResolvesEnv(t *testing.T) {
	t.Setenv("GH_TOKEN", "secret-value")
	store, err := NewSecretStore([]SecretSpec{
		{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"},
	})
	if err != nil {
		t.Fatalf("NewSecretStore: %v", err)
	}
	if h, v, ok := store.Header("gh"); !ok || h != "Authorization" || v != "secret-value" {
		t.Errorf("gh = (%q,%q,%v), want (Authorization,secret-value,true)", h, v, ok)
	}
	if _, _, ok := store.Header("missing"); ok {
		t.Error("missing secret resolved")
	}
}

func TestSecretStoreRejectsUnusableValues(t *testing.T) {
	if _, err := NewSecretStore([]SecretSpec{
		{Name: "gh", Header: "Authorization", ValueEnv: "SANDBOXD_TEST_UNSET_ENV"},
	}); err == nil {
		t.Error("NewSecretStore accepted an unset env var")
	}
	t.Setenv("SANDBOXD_TEST_EMPTY_ENV", "")
	if _, err := NewSecretStore([]SecretSpec{
		{Name: "gh", Header: "Authorization", ValueEnv: "SANDBOXD_TEST_EMPTY_ENV"},
	}); err == nil {
		t.Error("NewSecretStore accepted an empty env var as a credential")
	}
	t.Setenv("SANDBOXD_TEST_CTL_ENV", "tok\r\nX-Injected: 1")
	if _, err := NewSecretStore([]SecretSpec{
		{Name: "gh", Header: "Authorization", ValueEnv: "SANDBOXD_TEST_CTL_ENV"},
	}); err == nil {
		t.Error("NewSecretStore accepted a header-splitting value")
	}
}
