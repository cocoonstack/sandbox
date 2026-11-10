package config

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

const tunField = "tun"

var reloadableFields = map[string]bool{
	"egress_classes":                true,
	"egress_internal_allow":         true,
	"secrets":                       true,
	"egress_upstream":               true,
	"egress_usage_bytes":            true,
	"egress_usage_interval_seconds": true,
	"egress_conn_rate":              true,
	"disk_reserve":                  true,
	"memory_limit":                  true,

	"max_claims":                   true,
	"max_running":                  true,
	"move_concurrency":             true,
	"max_fork_count":               true,
	"release_delay_seconds":        true,
	"vmm_restart":                  true,
	"sync_claims":                  true,
	"checkpoint_ttl_hours":         true,
	"idle_hibernate_seconds":       true,
	"archive_after_seconds":        true,
	"archive_delete_after_seconds": true,
}

// ReloadDiff sorts next's differences from c into reloadable settings it changes and parts their own API owns, which a reload leaves alone; any other difference needs a restart and fails.
func (c *Config) ReloadDiff(next *Config) (changed, ignored []string, err error) {
	cur, nxt := reflect.ValueOf(c).Elem(), reflect.ValueOf(next).Elem()
	var restart []string
	for i := range cur.NumField() {
		name, _, _ := strings.Cut(cur.Type().Field(i).Tag.Get("json"), ",")
		switch {
		case name == "pools":
			ch, ig, rs := diffPools(c.Pools, next.Pools)
			changed, ignored, restart = append(changed, ch...), append(ignored, ig...), append(restart, rs...)
		case name == tunField:
			ch, rs := diffTun(c.Tun, next.Tun)
			changed, restart = append(changed, ch...), append(restart, rs...)
		case reflect.DeepEqual(cur.Field(i).Interface(), nxt.Field(i).Interface()):
		case reloadableFields[name]:
			changed = append(changed, name)
		default:
			restart = append(restart, name)
		}
	}
	if len(restart) > 0 {
		return nil, nil, fmt.Errorf("%s change only at a restart", strings.Join(restart, ", "))
	}
	return changed, ignored, nil
}

func diffPools(cur, next []PoolSpec) (changed, ignored, restart []string) {
	a, b := poolsByKey(cur), poolsByKey(next)
	all := poolsByKey(slices.Concat(cur, next))
	for _, k := range slices.SortedFunc(maps.Keys(all), func(x, y types.PoolKey) int { return strings.Compare(poolLabel(x), poolLabel(y)) }) {
		x, inA := a[k]
		y, inB := b[k]
		label := poolLabel(k)
		ys := y.ConfigOwned()
		for i, f := range x.ConfigOwned() {
			switch {
			case reflect.DeepEqual(f.Value.Interface(), ys[i].Value.Interface()):
			case f.Name == fieldReplicateSeconds:
				restart = append(restart, label+"."+f.Name)
			default:
				changed = append(changed, label+"."+f.Name)
			}
		}
		if inA != inB || targetsOf(x) != targetsOf(y) {
			ignored = append(ignored, label+" targets (PUT /v1/pools owns them)")
		}
	}
	return changed, ignored, restart
}

func diffTun(cur, next *TunConfig) (changed, restart []string) {
	switch {
	case reflect.DeepEqual(cur, next):
		return nil, nil
	case cur == nil || next == nil:
		return nil, []string{tunField}
	}
	for _, f := range []struct {
		name          string
		restart, same bool
	}{
		{"tun.subnet", true, cur.Prefix() == next.Prefix()},
		{"tun.listen", true, cur.ListenAddr() == next.ListenAddr()},
		{"tun.vni", true, cur.VNI == next.VNI},
		{"tun.senders", true, cur.Senders == next.Senders},
		{"tun.reuse_after_seconds", true, cur.ReuseAfterSeconds == next.ReuseAfterSeconds},
		{"tun.port", false, cur.Port == next.Port},
		{"tun.mtu", false, cur.MTU == next.MTU},
		{"tun.exits", false, maps.Equal(cur.Exits, next.Exits)},
		{"tun.limits", false, cur.Limits == next.Limits},
	} {
		switch {
		case f.same:
		case f.restart:
			restart = append(restart, f.name)
		default:
			changed = append(changed, f.name)
		}
	}
	return changed, restart
}

func targetsOf(s PoolSpec) [5]int {
	return [5]int{s.Warm, s.WarmMax, s.IdleHibernateSeconds, s.ArchiveAfterSeconds, s.ArchiveDeleteAfterSeconds}
}

func poolLabel(k types.PoolKey) string {
	return fmt.Sprintf("pools[%s %s %s]", k.Template, k.Net, k.Size)
}

func poolsByKey(specs []PoolSpec) map[types.PoolKey]PoolSpec {
	out := make(map[types.PoolKey]PoolSpec, len(specs))
	for _, s := range specs {
		out[s.PoolKey] = s
	}
	return out
}
