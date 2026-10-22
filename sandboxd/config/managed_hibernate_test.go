package config

import "testing"

func TestManagedHibernateHookValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hook  ManagedVolumeHibernate
		valid bool
	}{
		{"valid", ManagedVolumeHibernate{[]string{"/usr/local/bin/hook", "literal $(arg)"}, 10}, true},
		{"missing command", ManagedVolumeHibernate{nil, 10}, false},
		{"relative command", ManagedVolumeHibernate{[]string{"hook"}, 10}, false},
		{"NUL argument", ManagedVolumeHibernate{[]string{"/hook", "bad\x00arg"}, 10}, false},
		{"zero timeout", ManagedVolumeHibernate{[]string{"/hook"}, 0}, false},
		{"excess timeout", ManagedVolumeHibernate{[]string{"/hook"}, 301}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.hook.Validate(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
	cfg := Config{ManagedVolumeHibernate: &ManagedVolumeHibernate{[]string{"/hook"}, 1}}
	if err := cfg.ValidateManagedVolumeStore(); err == nil {
		t.Fatal("enabled hibernate without managed storage")
	}
}
