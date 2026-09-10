// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package interop

import (
	"io"
	"net"
	"testing"
	"time"

	legacy "example.com/yamux-legacy"
	current "github.com/fatedier/yamux"
)

// This test intentionally uses only the historical peer's public API. Ping
// provides receive-loop barriers without sleeps or access to private state.
func TestResetLegacyPeer(t *testing.T) {
	for _, role := range []string{"client", "server"} {
		for _, phase := range []string{"established", "before accept", "half closed"} {
			t.Run(role+"/"+phase, func(t *testing.T) {
				currentConn, legacyConn := net.Pipe()
				cfg := current.DefaultConfig()
				cfg.EnableKeepAlive = false
				cfg.StreamOpenTimeout = 0
				cfg.StreamCloseTimeout = 0
				cfg.ConnectionWriteTimeout = time.Second
				cfg.LogOutput = io.Discard
				oldCfg := legacy.DefaultConfig()
				oldCfg.EnableKeepAlive = false
				oldCfg.StreamOpenTimeout = 0
				oldCfg.StreamCloseTimeout = 0
				oldCfg.ConnectionWriteTimeout = time.Second
				oldCfg.LogOutput = io.Discard
				newCurrent, newLegacy := current.Client, legacy.Server
				if role == "server" {
					newCurrent, newLegacy = current.Server, legacy.Client
				}
				session, err := newCurrent(currentConn, cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = session.Close() })
				oldSession, err := newLegacy(legacyConn, oldCfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = oldSession.Close() })
				stream, err := session.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				var oldStream *legacy.Stream
				if phase != "before accept" {
					oldStream, err = oldSession.AcceptStream()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte("unread at old peer")); err != nil {
						t.Fatal(err)
					}
					if _, err := oldStream.Write([]byte("unread at new peer")); err != nil {
						t.Fatal(err)
					}
					if _, err := oldSession.Ping(); err != nil {
						t.Fatal(err)
					}
					if phase == "half closed" {
						if err := stream.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := stream.Reset(); err != nil {
					t.Fatal(err)
				}
				// The peer handles this ping after the RST on the same wire.
				if _, err := session.Ping(); err != nil {
					t.Fatal(err)
				}
				if phase == "before accept" {
					oldStream, err = oldSession.AcceptStream()
					if err != nil {
						t.Fatal(err)
					}
				}
				if n, err := stream.Read(make([]byte, 1)); n != 0 || err != current.ErrConnectionReset {
					t.Fatalf("new Read: %d, %v", n, err)
				}
				if n, err := oldStream.Read(make([]byte, 1)); n != 0 || err != legacy.ErrConnectionReset {
					t.Fatalf("old Read: %d, %v", n, err)
				}
				if n, err := oldStream.Write([]byte("x")); n != 0 || err != legacy.ErrConnectionReset {
					t.Fatalf("old Write: %d, %v", n, err)
				}
				if err := stream.Reset(); err != nil {
					t.Fatal(err)
				}
				if err := oldStream.Close(); err != nil {
					t.Fatal(err)
				}

				// Reset must leave the shared session usable in both directions.
				next, err := session.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				oldNext, err := oldSession.AcceptStream()
				if err != nil {
					t.Fatal(err)
				}
				for _, pair := range []struct {
					w io.Writer
					r io.Reader
				}{{next, oldNext}, {oldNext, next}} {
					if _, err := pair.w.Write([]byte("ok")); err != nil {
						t.Fatal(err)
					}
					buf := make([]byte, 2)
					if _, err := io.ReadFull(pair.r, buf); err != nil || string(buf) != "ok" {
						t.Fatalf("next stream: %q, %v", buf, err)
					}
				}
			})
		}
	}
}
