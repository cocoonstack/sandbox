package config

import "testing"

func TestClientAdvertise(t *testing.T) {
	for _, addr := range []string{"", "https://node.example", "https://node.example:8443/", "http://[::1]:7777"} {
		t.Run(addr, func(t *testing.T) {
			cfg := Config{ClientAdvertise: addr}
			if err := cfg.validateClientAdvertise(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, addr := range []string{"node:7777", "https://0.0.0.0", "https://[::]", "https://", "ftp://node", "https://u:p@node", "https://node/path", "https://node?x=1", "https://node#", "https://node#x", "https://node:0", "https://node:65536"} {
		t.Run(addr, func(t *testing.T) {
			cfg := Config{ClientAdvertise: addr}
			if err := cfg.validateClientAdvertise(); err == nil {
				t.Fatalf("accepted %q", addr)
			}
		})
	}
}
