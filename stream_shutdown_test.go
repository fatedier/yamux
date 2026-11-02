// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Delay transport teardown after shutdownCh closes, as a blocked transport
// Close does in production. Reads and receive-frame publication remain live.
type shutdownCloseConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *shutdownCloseConn) Close() error {
	close(c.started)
	<-c.release
	return c.Conn.Close()
}

func (c *shutdownCloseConn) unblock() { c.once.Do(func() { close(c.release) }) }

func newShutdownWireStream(t *testing.T) (*Stream, net.Conn, func(), <-chan error) {
	t.Helper()
	local, peer := net.Pipe()
	conn := &shutdownCloseConn{Conn: local, started: make(chan struct{}), release: make(chan struct{})}
	cfg := testConfNoKeepAlive()
	cfg.LogOutput = io.Discard
	cfg.StreamOpenTimeout = 0
	cfg.StreamCloseTimeout = 0
	s, err := Client(conn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	stream := newStream(s, 1, streamEstablished)
	s.streamLock.Lock()
	s.streams[stream.id] = stream
	s.streamLock.Unlock()
	t.Cleanup(func() { conn.unblock(); _ = peer.Close(); _ = s.Close() })
	closed := make(chan error, 1)
	startClose := func() {
		go func() { closed <- s.Close() }()
		select {
		case <-conn.started:
		case <-time.After(time.Second):
			t.Fatal("Session.Close did not reach transport Close")
		}
	}
	return stream, peer, startClose, closed
}

func waitShutdownReceive(t *testing.T, stream *Stream, buffered int, inFlight bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		stream.recvLock.Lock()
		n := 0
		if stream.recvBuf != nil {
			n = stream.recvBuf.Len()
		}
		ready := n == buffered && stream.recvInFlight == inFlight
		stream.recvLock.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("receive frame did not reach the expected publication state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamSessionShutdownRead(t *testing.T) {
	for _, buffered := range []string{"", "buffered before shutdown"} {
		for _, finished := range []bool{false, true} {
			t.Run(buffered+"/teardown="+map[bool]string{false: "blocked", true: "finished"}[finished], func(t *testing.T) {
				stream, peer, closeSession, closed := newShutdownWireStream(t)
				writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte(buffered))
				waitShutdownReceive(t, stream, len(buffered), false)
				closeSession()
				if finished {
					stream.session.conn.(*shutdownCloseConn).unblock()
					if err := awaitSendResult(t, closed); err != nil {
						t.Fatal(err)
					}
				}
				var got []byte
				read := resetAsync(func() error {
					var err error
					got, err = io.ReadAll(stream)
					return err
				})
				if err := awaitSendResult(t, read); err != ErrSessionShutdown || string(got) != buffered {
					t.Fatalf("ReadAll = %q, %v; want %q, ErrSessionShutdown", got, err, buffered)
				}
				// A retained receive dispatch cannot publish a new frame after
				// Read has reported shutdown; already admitted frames are tested below.
				h := header(make([]byte, headerSize))
				h.encode(typeData, 0, stream.id, 4)
				late := bytes.NewReader([]byte("late"))
				if err := stream.readData(h, 0, late); err != nil {
					t.Fatal(err)
				}
				if late.Len() != 0 {
					t.Fatal("late frame was not drained")
				}
				waitShutdownReceive(t, stream, 0, false)
				for range 3 {
					if n, err := stream.Read(make([]byte, 4)); n != 0 || err != ErrSessionShutdown {
						t.Fatalf("Read after shutdown = %d, %v", n, err)
					}
				}
				t.Log("late DATA drained without publication; three subsequent Reads returned 0, ErrSessionShutdown")
			})
		}
	}
}

func TestStreamSessionShutdownPartialFrame(t *testing.T) {
	for _, finish := range []string{"complete", "transport failure", "reset"} {
		t.Run(finish, func(t *testing.T) {
			stream, peer, closeSession, _ := newShutdownWireStream(t)
			writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("buffered"))
			waitShutdownReceive(t, stream, 8, false)
			h := header(make([]byte, headerSize))
			h.encode(typeData, 0, stream.id, 7)
			if err := writeFrame(peer, append(h, []byte("par")...)); err != nil {
				t.Fatal(err)
			}
			waitShutdownReceive(t, stream, 8, true)
			t.Log("before shutdown: recvInFlight=true, 8 published bytes, 7-byte frame has received par")
			closeSession()
			buf := make([]byte, 8)
			if n, err := stream.Read(buf); n != 8 || err != nil || string(buf) != "buffered" {
				t.Fatalf("published bytes = %q, %d, %v", buf, n, err)
			}
			var got []byte
			read := resetAsync(func() error {
				var err error
				got, err = io.ReadAll(stream)
				return err
			})
			select {
			case err := <-read:
				t.Fatalf("Read returned before the admitted frame finished: %q, %v", got, err)
			case <-time.After(10 * time.Millisecond):
			}
			wantData, wantErr := "partial", ErrSessionShutdown
			switch finish {
			case "complete":
				if err := writeFrame(peer, []byte("tial")); err != nil {
					t.Fatal(err)
				}
			case "transport failure":
				wantData = "par"
				_ = peer.Close()
			case "reset":
				wantData, wantErr = "", ErrConnectionReset
				if err := stream.Reset(); err != ErrSessionShutdown {
					t.Fatalf("Reset's unsent RST = %v", err)
				}
			}
			if err := awaitSendResult(t, read); err != wantErr || string(got) != wantData {
				t.Fatalf("ReadAll = %q, %v; want %q, %v", got, err, wantData, wantErr)
			}
			t.Logf("after shutdown: published bytes preserved; frame result = %q, %v", got, wantErr)
		})
	}
}

func TestStreamSessionShutdownTerminalPriority(t *testing.T) {
	for _, terminal := range []string{"reset", "FIN", "local close"} {
		t.Run(terminal, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, _ := newResetTestStream(t)
				read := resetAsync(func() error { _, err := stream.Read(make([]byte, 1)); return err })
				synctest.Wait()
				// Hold the terminal-state lock across both publications so no
				// waiter can legitimately finish before the terminal state exists.
				stream.stateLock.Lock()
				readErr := ErrConnectionReset
				switch terminal {
				case "reset":
					stream.resetLocked()
				case "FIN":
					stream.state = streamRemoteClose
					readErr = io.EOF
				case "local close":
					stream.state = streamLocalClose
					readErr = ErrSessionShutdown
				}
				stream.session.shutdownLock.Lock()
				stream.session.shutdown = true
				close(stream.session.shutdownCh)
				stream.session.shutdownLock.Unlock()
				stream.stateLock.Unlock()
				stream.notifyWaiting()
				if err := awaitSendResult(t, read); err != readErr {
					t.Fatalf("Read = %v, want %v", err, readErr)
				}
				stream.forceClose()
				if terminal == "reset" {
					if n, err := stream.Read(nil); n != 0 || err != ErrConnectionReset {
						t.Fatalf("Read after teardown = %d, %v", n, err)
					}
				}
			})
		})
	}
}

func TestStreamSessionShutdownFINBuffered(t *testing.T) {
	stream, peer, closeSession, _ := newShutdownWireStream(t)
	writeResetWireFrame(t, peer, typeData, flagFIN, stream.id, []byte("normal FIN"))
	// Wait for the complete frame and FIN to be published before shutdown.
	deadline := time.Now().Add(time.Second)
	for {
		stream.stateLock.Lock()
		finished := stream.state == streamRemoteClose
		stream.stateLock.Unlock()
		if finished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FIN was not published")
		}
		time.Sleep(time.Millisecond)
	}
	closeSession()
	var got []byte
	read := resetAsync(func() error { var err error; got, err = io.ReadAll(stream); return err })
	if err := awaitSendResult(t, read); err != nil || !bytes.Equal(got, []byte("normal FIN")) {
		t.Fatalf("FIN ReadAll = %q, %v", got, err)
	}
}

func TestStreamSessionShutdownPreservesGracefulEOF(t *testing.T) {
	stream, peer, closeSession, closed := newShutdownWireStream(t)
	closing := resetAsync(stream.Close)
	if h := readResetWireHeader(t, peer); h.Flags() != flagFIN {
		t.Fatalf("local Close sent %v, want FIN", h)
	}
	if err := awaitSendResult(t, closing); err != nil {
		t.Fatal(err)
	}
	writeResetWireFrame(t, peer, typeWindowUpdate, flagFIN, stream.id, nil)
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read after FIN handshake = %d, %v", n, err)
	}
	stream.stateLock.Lock()
	state, terminal := stream.state, stream.terminal
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal {
		t.Fatalf("FIN handshake state = %v, terminal = %v", state, terminal)
	}
	closeSession()
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read during session teardown = %d, %v", n, err)
	}
	stream.session.conn.(*shutdownCloseConn).unblock()
	if err := awaitSendResult(t, closed); err != nil {
		t.Fatal(err)
	}
	// The completed handshake unregisters the stream, so exercise teardown
	// on a retained pointer too. It must not replace an established EOF.
	stream.forceClose()
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read after session teardown = %d, %v", n, err)
	}
	t.Log("streamClosed/terminal EOF survived session teardown and forceClose on a retained pointer")
}
