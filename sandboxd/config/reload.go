package config

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/cocoonstack/sandbox/sandboxd/types"
)

var reloadableFields = map[string]bool{
	"egress_classes":        true,
	"egress_internal_allow": true,
	"secrets":               true,
	"egress_upstream":       true,
	"egress_usage_bytes":    true,
}

type poolTargets struct {
	warm, warmMax, idle, archiveAfter, archiveDelete int
}

// ReloadDiff sorts next's differences from c into reloadable settings it changes and parts their own API owns, which a reload leaves alone; any other difference needs a restart and fails.
func (c *Config) ReloadDiff(next *Config) (changed, ignored []string, err error) {
	cur, nxt := reflect.ValueOf(c).Elem(), reflect.ValueOf(next).Elem()
	var restart []string
	for i := range cur.NumField() {
		name, _, _ := strings.Cut(cur.Type().Field(i).Tag.Get("json"), ",")
		switch {
		case name == "pools":
			ch, ig := diffPools(c.Pools, next.Pools)
			changed, ignored = append(changed, ch...), append(ignored, ig...)
		case name == "tenants":
			ignored = append(ignored, diffTenants(c.Tenants, next.Tenants)...)
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

func diffPools(cur, next []PoolSpec) (changed, ignored []string) {
	byKey := func(s PoolSpec) types.PoolKey { return s.PoolKey }
	a, b := indexBy(cur, byKey), indexBy(next, byKey)
	all := indexBy(slices.Concat(cur, next), byKey)
	for _, k := range slices.SortedFunc(maps.Keys(all), func(x, y types.PoolKey) int { return strings.Compare(poolLabel(x), poolLabel(y)) }) {
		x, inA := a[k]
		y, inB := b[k]
		label := poolLabel(k)
		for _, f := range []struct {
			name string
			same bool
		}{
			{"egress", reflect.DeepEqual(x.Egress, y.Egress)},
			{"warmup", slices.Equal(x.Warmup, y.Warmup)},
			{"capture_trim", x.CaptureTrim == y.CaptureTrim},
			{"storage", x.Storage == y.Storage},
			{"egress_upstream_env", x.EgressUpstreamEnv == y.EgressUpstreamEnv},
		} {
			if !f.same {
				changed = append(changed, label+"."+f.name)
			}
		}
		if inA != inB || targetsOf(x) != targetsOf(y) {
			ignored = append(ignored, label+" targets (PUT /v1/pools owns them)")
		}
	}
	return changed, ignored
}

func diffTenants(cur, next []TenantSpec) (ignored []string) {
	byName := func(s TenantSpec) string { return s.Name }
	a, b := indexBy(cur, byName), indexBy(next, byName)
	for _, name := range slices.Sorted(maps.Keys(indexBy(slices.Concat(cur, next), byName))) {
		if a[name] != b[name] {
			ignored = append(ignored, "tenants["+name+"] identity (/v1/tenants owns it)")
		}
	}
	return ignored
}

func targetsOf(s PoolSpec) poolTargets {
	return poolTargets{s.Warm, s.WarmMax, s.IdleHibernateSeconds, s.ArchiveAfterSeconds, s.ArchiveDeleteAfterSeconds}
}

func poolLabel(k types.PoolKey) string {
	return fmt.Sprintf("pools[%s %s %s]", k.Template, k.Net, k.Size)
}

func indexBy[K comparable, V any](specs []V, key func(V) K) map[K]V {
	out := make(map[K]V, len(specs))
	for _, s := range specs {
		out[key(s)] = s
	}
	return out
}
