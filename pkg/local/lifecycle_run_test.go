// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package local

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLifecycle_Run_DeletesOldFiles(t *testing.T) {
	dir := t.TempDir()
	s := New()
	// Use in-memory lifecycle manager (default)
	if err := s.Configure(map[string]string{"path": dir}, noCreds); err != nil {
		t.Fatal(err)
	}
	ll := s.(*Local)

	// Ensure we're using the in-memory lifecycle manager for this test
	memManager, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	// create old file
	key := "logs/old.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, key)
	ageObject(t, ll, dir, key, time.Now().Add(-48*time.Hour))

	// policy to delete under logs older than 1h
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "run", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	// speed up interval and run
	memManager.interval = 10 * time.Millisecond
	processed := make(chan struct{}, 1)
	memManager.onProcessed = func() {
		select {
		case processed <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { memManager.Run(ctx, ll); close(done) }()

	// wait for the first completed Process pass, then stop the loop
	<-processed
	cancel()
	<-done
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted, got err=%v", err)
	}
}

// TestLocal_RunLifecycle_PersistentPolicySurvivesRestart adds a persistent
// policy, closes the store, reopens it on the same root with runLifecycle,
// and checks that one scheduled pass deletes an expired object (#224).
func TestLocal_RunLifecycle_PersistentPolicySurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	first := new(Local)
	if err := first.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
	}, noCreds); err != nil {
		t.Fatal(err)
	}
	if err := first.AddPolicy(common.LifecyclePolicy{ID: "expire-logs", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}
	key := "logs/old.txt"
	if err := first.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	keep := "data/old.txt"
	if err := first.Put(keep, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	for _, k := range []string{key, keep} {
		ageObject(t, first, dir, k, old)
	}
	first.Close()

	second := new(Local)
	if err := second.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"runLifecycle":         "true",
		"lifecycleInterval":    "20ms",
	}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	processed := make(chan struct{}, 1)
	hook := func() {
		select {
		case processed <- struct{}{}:
		default:
		}
	}
	second.lifecyclePassHook.Store(&hook)

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("no scheduled lifecycle pass completed")
	}
	second.Close()

	if _, err := os.Stat(filepath.Join(dir, key)); !os.IsNotExist(err) {
		t.Fatalf("expected %s deleted by the scheduled pass, got err=%v", key, err)
	}
	if _, err := os.Stat(filepath.Join(dir, keep)); err != nil {
		t.Fatalf("expected %s kept (outside the policy prefix), got err=%v", keep, err)
	}
}

// TestLocal_LifecycleInterval_Setting checks the default and the refusals
// of the lifecycleInterval setting.
func TestLocal_LifecycleInterval_Setting(t *testing.T) {
	cases := []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: "", want: time.Hour},
		{raw: "5m", want: 5 * time.Minute},
		{raw: "0", wantErr: true},
		{raw: "-1s", wantErr: true},
		{raw: "soon", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			s := new(Local)
			settings := map[string]string{"path": t.TempDir()}
			if tc.raw != "" {
				settings["lifecycleInterval"] = tc.raw
			}
			err := s.Configure(settings, noCreds)
			defer s.Close()
			if tc.wantErr {
				if !errors.Is(err, common.ErrInvalidArgument) {
					t.Fatalf("Configure(lifecycleInterval=%q) err = %v, want ErrInvalidArgument", tc.raw, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if s.lifecycleInterval != tc.want {
				t.Fatalf("lifecycleInterval = %v, want %v", s.lifecycleInterval, tc.want)
			}
		})
	}
}

// TestLocal_RunLifecycle_Off checks that no scheduled pass starts without
// runLifecycle.
func TestLocal_RunLifecycle_Off(t *testing.T) {
	s := new(Local)
	if err := s.Configure(map[string]string{
		"path":                 t.TempDir(),
		"lifecycleManagerType": "persistent",
		"runLifecycle":         "false",
	}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.lifecycleCancel != nil {
		t.Fatal("expected no scheduled lifecycle pass with runLifecycle=false")
	}
}
