package main

import (
	"strings"
	"testing"

	"github.com/cocoonstack/sandbox/sandboxd/config"
	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/fallback"
	"github.com/cocoonstack/sandbox/sandboxd/store/s3"
)

func TestAStoreWithoutAFallbackIsThePlainS3Store(t *testing.T) {
	t.Setenv("SBX_TEST_R2_ID", "id")
	t.Setenv("SBX_TEST_R2_SECRET", "secret")
	gcs := store.S3Config{Bucket: "old", Prefix: "ck/", Region: "auto"}
	r2 := store.S3Config{Bucket: "new", Prefix: "ck/", Region: "auto", AccessKeyIDEnv: "SBX_TEST_R2_ID", SecretAccessKeyEnv: "SBX_TEST_R2_SECRET"}
	cfg := func(fb *store.S3Config) *config.Config {
		primary := r2
		if fb == nil {
			primary = gcs
		}
		return &config.Config{DataDir: t.TempDir(), CheckpointStore: &config.StoreConfig{Kind: "s3", S3: &primary, Fallback: fb}}
	}
	st, err := newStoreView(t.Context(), cfg(nil), "checkpoints", "checkpoint-staging", store.CheckpointIDRe, false)
	if _, plain := st.(*s3.Store); err != nil || !plain {
		t.Fatalf("store without a fallback = %T, %v; want the plain *s3.Store", st, err)
	}

	withKey := gcs
	withKey.AccessKeyIDEnv, withKey.SecretAccessKeyEnv = "SBX_TEST_R2_ID", "SBX_TEST_R2_SECRET"
	st, err = newStoreView(t.Context(), cfg(&withKey), "checkpoints", "checkpoint-staging", store.CheckpointIDRe, false)
	if _, wrapped := st.(*fallback.Store); err != nil || !wrapped {
		t.Fatalf("store with a fallback = %T, %v; want a *fallback.Store", st, err)
	}

	withKey.SecretAccessKeyEnv = "SBX_TEST_UNSET"
	if _, err = newStoreView(t.Context(), cfg(&withKey), "checkpoints", "checkpoint-staging", store.CheckpointIDRe, false); err == nil || !strings.Contains(err.Error(), "SBX_TEST_UNSET") {
		t.Errorf("a fallback whose key env is empty: %v, want a start error naming it", err)
	}
}
