// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestStreamWriteSessionShutdown(t *testing.T) {
	stream, peer, closeSession, _ := newShutdownWireStream(t)
	atomic.StoreUint32(&stream.sendWindow, 5)
	var n int
	for _, payload := range [][]byte{[]byte("sent"), []byte("!")} {
		first := resetAsync(func() error {
			var err error
			n, err = stream.Write(payload)
			return err
		})
		if h := readResetWireHeader(t, peer); h.MsgType() != typeData || h.Length() != uint32(len(payload)) {
			t.Fatalf("DATA header: %v", h)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(peer, got); err != nil {
			t.Fatal(err)
		}
		if err := awaitSendResult(t, first); err != nil || n != len(payload) || !bytes.Equal(got, payload) {
			t.Fatalf("Write with credit = %d, %v; wire = %q", n, err, got)
		}
	}
	if window := atomic.LoadUint32(&stream.sendWindow); window != 0 {
		t.Fatalf("send window = %d, want exhausted", window)
	}
	write := resetAsync(func() error {
		var err error
		n, err = stream.Write([]byte("unsent"))
		return err
	})
	select {
	case err := <-write:
		t.Fatalf("Write returned before window credit or shutdown: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	closeSession() // shutdownCh is closed, but transport Close remains blocked.
	if err := awaitSendResult(t, write); err != ErrSessionShutdown || n != 0 {
		t.Fatalf("Write without credit after shutdown = %d, %v", n, err)
	}
}

func TestStreamWriteSessionShutdownTerminalPriority(t *testing.T) {
	for _, terminal := range []string{"reset", "closed", "local close"} {
		for _, order := range []string{"terminal first", "shutdown first"} {
			t.Run(terminal+"/"+order, func(t *testing.T) {
				resetSynctest(t, func(t *testing.T) {
					stream, _ := newResetTestStream(t)
					atomic.StoreUint32(&stream.sendWindow, 0)
					var n int
					write := resetAsync(func() error {
						var err error
						n, err = stream.Write([]byte("unsent"))
						return err
					})
					synctest.Wait()
					// Both events must be published before START can inspect the
					// state; a Reset published after Write returns cannot win.
					stream.stateLock.Lock()
					publishTerminal := func() {
						switch terminal {
						case "reset":
							stream.resetLocked()
							close(stream.resetCh)
						case "closed":
							stream.forceCloseLocked()
						case "local close":
							stream.state = streamLocalClose
						}
					}
					publishShutdown := func() {
						stream.session.shutdownLock.Lock()
						stream.session.shutdown = true
						close(stream.session.shutdownCh)
						stream.session.shutdownLock.Unlock()
					}
					if order == "terminal first" {
						publishTerminal()
						publishShutdown()
					} else {
						publishShutdown()
						publishTerminal()
					}
					stream.stateLock.Unlock()
					stream.notifyWaiting()
					want := ErrStreamClosed
					if terminal == "reset" {
						want = ErrConnectionReset
					}
					if err := awaitSendResult(t, write); err != want || n != 0 {
						t.Fatalf("Write = %d, %v; want 0, %v", n, err, want)
					}
				})
			})
		}
	}
}

func TestStreamWriteWindowDeadline(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, _ := newResetTestStream(t)
		atomic.StoreUint32(&stream.sendWindow, 0)
		if err := stream.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if n, err := stream.Write([]byte("unsent")); n != 0 || err != ErrTimeout {
			t.Fatalf("Write without credit at deadline = %d, %v", n, err)
		}
	})
}
