package egress

import "testing"

func TestSecretSpecValidate(t *testing.T) {
	tests := []struct {
		name string
		spec SecretSpec
		ok   bool
	}{
		{"literal", SecretSpec{Name: "gh", Header: "Authorization", Value: "tok"}, true},
		{"env", SecretSpec{Name: "gh", Header: "Authorization", ValueEnv: "GH_TOKEN"}, true},
		{"no name", SecretSpec{Header: "Authorization", Value: "tok"}, false},
		{"no header", SecretSpec{Name: "gh", Value: "tok"}, false},
		{"no value", SecretSpec{Name: "gh", Header: "Authorization"}, false},
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
		{Name: "lit", Header: "X-Key", Value: "plain"},
	})
	if err != nil {
		t.Fatalf("NewSecretStore: %v", err)
	}
	if h, v, ok := store.Header("gh"); !ok || h != "Authorization" || v != "secret-value" {
		t.Errorf("gh = (%q,%q,%v), want (Authorization,secret-value,true)", h, v, ok)
	}
	if _, v, ok := store.Header("lit"); !ok || v != "plain" {
		t.Errorf("lit value = (%q,%v), want (plain,true)", v, ok)
	}
	if _, _, ok := store.Header("missing"); ok {
		t.Error("missing secret resolved")
	}
}

func TestSecretStoreUnsetEnvFails(t *testing.T) {
	if _, err := NewSecretStore([]SecretSpec{
		{Name: "gh", Header: "Authorization", ValueEnv: "SANDBOXD_TEST_UNSET_ENV"},
	}); err == nil {
		t.Error("NewSecretStore accepted an unset env var")
	}
}
