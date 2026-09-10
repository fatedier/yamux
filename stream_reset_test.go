// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// Timers cannot cross synctest bubbles. These tests are deliberately serial;
// give each bubble a private timer cache and restore the real-time cache after
// all its goroutines have exited.
func resetSynctest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	original := timerPool
	timerPool = &sync.Pool{New: original.New}
	defer func() { timerPool = original }()
	synctest.Test(t, f)
}

func newResetTestStream(t *testing.T) (*Stream, *controlledWriteConn) {
	t.Helper()
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	prepareSessionClose(t, session)
	stream := newStream(session, 1, streamEstablished)
	session.streams[stream.id] = stream
	go session.send()
	t.Cleanup(func() { _ = session.Close() })
	return stream, conn
}

func resetAsync(f func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- f() }()
	return ch
}

func assertResetIO(t *testing.T, stream *Stream) {
	t.Helper()
	for _, b := range [][]byte{nil, make([]byte, 1)} {
		if n, err := stream.Read(b); n != 0 || err != ErrConnectionReset {
			t.Fatalf("Read(%d): %d, %v", len(b), n, err)
		}
		if n, err := stream.Write(b); n != 0 || err != ErrConnectionReset {
			t.Fatalf("Write(%d): %d, %v", len(b), n, err)
		}
	}
	stream.recvLock.Lock()
	defer stream.recvLock.Unlock()
	if stream.recvBuf != nil {
		t.Fatal("reset retained the receive buffer")
	}
}

func assertRST(t *testing.T, frame []byte, id uint32) {
	t.Helper()
	h := header(frame)
	if len(h) != headerSize || h.Version() != 0 || h.MsgType() != typeWindowUpdate || h.Flags() != flagRST || h.StreamID() != id || h.Length() != 0 {
		t.Fatalf("unexpected RST: %v", h)
	}
}

// A ping after a canceled/reset stream provides a positive wire barrier: any
// stray stream frame or duplicate RST will be observed before this ping.
func resetWireBarrier(t *testing.T, s *Session, conn *controlledWriteConn) {
	t.Helper()
	h := header(make([]byte, headerSize))
	h.encode(typePing, flagACK, 0, 123)
	done := resetAsync(func() error { return s.waitForSend(h, nil) })
	frame := awaitControlledWrite(t, conn)
	if !bytes.Equal(frame, h) {
		t.Fatalf("unexpected frame before wire barrier: %v", header(frame))
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
	if err := awaitSendResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func blockResetTransport(t *testing.T, s *Session, conn *controlledWriteConn) {
	t.Helper()
	h := header(make([]byte, headerSize))
	h.encode(typePing, flagACK, 0, 1)
	if err := s.sendNoWait(h); err != nil {
		t.Fatal(err)
	}
	if got := awaitControlledWrite(t, conn); !bytes.Equal(got, h) {
		t.Fatalf("unexpected blocker: %v", got)
	}
}

func TestStreamResetConcurrentResult(t *testing.T) {
	transportErr := errors.New("RST transport failure")
	for _, tc := range []struct {
		name   string
		result controlledWriteResult
		want   error
	}{
		{"success", controlledWriteResult{n: headerSize}, nil},
		{"transport failure", controlledWriteResult{err: transportErr}, transportErr},
		{"short write", controlledWriteResult{n: headerSize - 1}, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				stream.recvBuf = bytes.NewBufferString("unread")
				first := resetAsync(stream.Reset)
				assertRST(t, awaitControlledWrite(t, conn), stream.id)
				const callers = 16
				results := make([]<-chan error, callers+1)
				results[0] = first
				for i := 1; i < len(results); i++ {
					results[i] = resetAsync(stream.Reset)
				}
				synctest.Wait()
				assertResetIO(t, stream)
				if stream.session.NumStreams() != 0 {
					t.Fatal("reset did not unregister immediately")
				}
				// Queue deadlines must not turn a committed RST into a timeout.
				time.Sleep(2 * stream.session.config.ConnectionWriteTimeout)
				for _, result := range results {
					select {
					case err := <-result:
						t.Fatalf("Reset returned before transport completion: %v", err)
					default:
					}
				}
				releaseControlledWrite(t, conn, tc.result)
				for _, result := range results {
					if err := awaitSendResult(t, result); err != tc.want {
						t.Fatalf("Reset = %v, want %v", err, tc.want)
					}
				}
				if err := stream.Reset(); err != tc.want {
					t.Fatalf("repeated Reset = %v", err)
				}
				if err := stream.Close(); err != nil {
					t.Fatalf("Close after Reset = %v", err)
				}
				assertResetIO(t, stream)
				if tc.want == nil {
					resetWireBarrier(t, stream.session, conn)
				} else {
					synctest.Wait()
					if !stream.session.IsClosed() {
						t.Fatal("transport failure did not close the session")
					}
				}
			})
		})
	}
}

func TestStreamResetQueueFailure(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "queued"
		if full {
			name = "queue full"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				blockResetTransport(t, stream.session, conn)
				var filler *sendReady
				if full {
					h := header(make([]byte, headerSize))
					h.encode(typePing, flagACK, 0, 2)
					filler = newSendReady(h)
					for i := 0; i < cap(stream.session.sendCh); i++ {
						stream.session.sendCh <- filler
					}
				}
				first := resetAsync(stream.Reset)
				second := resetAsync(stream.Reset)
				synctest.Wait()
				assertResetIO(t, stream)
				time.Sleep(stream.session.config.ConnectionWriteTimeout)
				for _, result := range []<-chan error{first, second} {
					if err := awaitSendResult(t, result); err != ErrConnectionWriteTimeout {
						t.Fatalf("queue failure: %v", err)
					}
				}
				if err := stream.Reset(); err != ErrConnectionWriteTimeout {
					t.Fatalf("repeated Reset: %v", err)
				}
				if filler != nil {
					filler.cancel()
				}
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				resetWireBarrier(t, stream.session, conn)
				assertResetIO(t, stream)
			})
		})
	}
}

func TestStreamResetBlockedIO(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "flow control"
		if queued {
			name = "queued write"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				blockResetTransport(t, stream.session, conn)
				if !queued {
					atomic.StoreUint32(&stream.sendWindow, 0)
				}
				read := resetAsync(func() error {
					n, err := stream.Read(make([]byte, 1))
					if n != 0 {
						t.Errorf("blocked Read returned %d bytes", n)
					}
					return err
				})
				write := resetAsync(func() error {
					n, err := stream.Write([]byte("unsent"))
					if n != 0 {
						t.Errorf("blocked Write returned %d bytes", n)
					}
					return err
				})
				synctest.Wait()
				reset := resetAsync(stream.Reset)
				synctest.Wait()
				for _, result := range []<-chan error{read, write} {
					if err := awaitSendResult(t, result); err != ErrConnectionReset {
						t.Fatalf("blocked I/O = %v", err)
					}
				}
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				assertRST(t, awaitControlledWrite(t, conn), stream.id)
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				if err := awaitSendResult(t, reset); err != nil {
					t.Fatal(err)
				}
				resetWireBarrier(t, stream.session, conn)
			})
		})
	}
}

func TestStreamResetCommittedWrite(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "complete"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				atomic.StoreUint32(&stream.sendWindow, 3)
				payload := []byte("abc")
				if partial {
					payload = []byte("abcdef")
				}
				var n int
				write := resetAsync(func() error {
					var err error
					n, err = stream.Write(payload)
					return err
				})
				frame := awaitControlledWrite(t, conn)
				if header(frame).MsgType() != typeData || header(frame).Length() != 3 {
					t.Fatalf("data header: %v", frame)
				}
				reset := resetAsync(stream.Reset)
				synctest.Wait()
				select {
				case err := <-write:
					t.Fatalf("committed Write returned early: %v", err)
				default:
				}
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				if body := awaitControlledWrite(t, conn); string(body) != "abc" {
					t.Fatalf("body: %q", body)
				}
				releaseControlledWrite(t, conn, controlledWriteResult{n: 3})
				want := error(nil)
				if partial {
					want = ErrConnectionReset
				}
				if err := awaitSendResult(t, write); err != want || n != 3 {
					t.Fatalf("Write = %d, %v", n, err)
				}
				assertRST(t, awaitControlledWrite(t, conn), stream.id)
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				if err := awaitSendResult(t, reset); err != nil {
					t.Fatal(err)
				}
				assertResetIO(t, stream)
				resetWireBarrier(t, stream.session, conn)
			})
		})
	}
}

func TestStreamResetSessionClose(t *testing.T) {
	for _, phase := range []string{"before reset", "RST queued", "RST committed", "after reset"} {
		t.Run(phase, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				if phase == "before reset" {
					if err := stream.session.Close(); err != nil {
						t.Fatal(err)
					}
					if err := stream.Reset(); err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Read(make([]byte, 1)); err != io.EOF {
						t.Fatalf("Read = %v", err)
					}
					if _, err := stream.Write([]byte("x")); err != ErrStreamClosed {
						t.Fatalf("Write = %v", err)
					}
					return
				}
				if phase == "RST queued" {
					blockResetTransport(t, stream.session, conn)
				}
				reset := resetAsync(stream.Reset)
				synctest.Wait()
				want := error(nil)
				if phase == "RST queued" {
					want = ErrSessionShutdown
				} else {
					assertRST(t, awaitControlledWrite(t, conn), stream.id)
					if phase == "after reset" {
						releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
					} else {
						want = net.ErrClosed
					}
				}
				if phase == "after reset" {
					if err := awaitSendResult(t, reset); err != nil {
						t.Fatal(err)
					}
				}
				if err := stream.session.Close(); err != nil {
					t.Fatal(err)
				}
				if phase != "after reset" {
					if err := awaitSendResult(t, reset); err != want {
						t.Fatalf("Reset = %v, want %v", err, want)
					}
				}
				// Model a Session.Close snapshot taken before Reset removed the map entry.
				stream.forceClose()
				assertResetIO(t, stream)
				if err := stream.Reset(); err != want {
					t.Fatalf("repeated Reset = %v", err)
				}
			})
		})
	}
}

func TestStreamResetConcurrentClose(t *testing.T) {
	for _, phase := range []string{"FIN queued", "FIN committed", "half closed", "remote FIN", "fully closed"} {
		t.Run(phase, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				stream.session.config.StreamCloseTimeout = time.Hour
				if phase == "remote FIN" || phase == "fully closed" {
					if err := stream.processFlags(flagFIN); err != nil {
						t.Fatal(err)
					}
				}
				var closeResult <-chan error
				if phase == "FIN queued" {
					blockResetTransport(t, stream.session, conn)
				}
				if phase != "remote FIN" {
					closeResult = resetAsync(stream.Close)
					synctest.Wait()
					if phase != "FIN queued" {
						if frame := awaitControlledWrite(t, conn); header(frame).Flags() != flagFIN {
							t.Fatalf("FIN: %v", frame)
						}
						if phase != "FIN committed" {
							releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
							if err := awaitSendResult(t, closeResult); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if phase == "fully closed" {
					if err := stream.Reset(); err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Read(make([]byte, 1)); err != io.EOF {
						t.Fatalf("Read = %v", err)
					}
					resetWireBarrier(t, stream.session, conn)
					return
				}
				reset := resetAsync(stream.Reset)
				synctest.Wait()
				if phase == "FIN queued" {
					if err := awaitSendResult(t, closeResult); err != ErrConnectionReset {
						t.Fatalf("queued Close = %v", err)
					}
				}
				if phase == "FIN queued" || phase == "FIN committed" {
					releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
					if phase == "FIN committed" {
						if err := awaitSendResult(t, closeResult); err != nil {
							t.Fatalf("committed Close = %v", err)
						}
					}
				}
				assertRST(t, awaitControlledWrite(t, conn), stream.id)
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				if err := awaitSendResult(t, reset); err != nil {
					t.Fatal(err)
				}
				// A stopped but already running timer must share the RST attempt.
				stream.closeTimeout()
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				assertResetIO(t, stream)
				if stream.closeTimer != nil || stream.closePending {
					t.Fatal("close lifecycle survived reset")
				}
				resetWireBarrier(t, stream.session, conn)
			})
		})
	}
}

func TestStreamResetBeforeSessionTeardown(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		// Hold Session.Close between closing the transport and force-closing
		// its streams, as a receive loop finishing its last frame would do.
		stream.session.recvDoneCh = make(chan struct{})
		closed := resetAsync(stream.session.Close)
		synctest.Wait()
		select {
		case <-conn.closed:
		default:
			t.Fatal("Session.Close has not reached receive teardown")
		}
		if err := stream.Reset(); err != ErrSessionShutdown {
			t.Fatalf("Reset = %v", err)
		}
		close(stream.session.recvDoneCh)
		if err := awaitSendResult(t, closed); err != nil {
			t.Fatal(err)
		}
		assertResetIO(t, stream)
	})
}

func TestStreamResetReadWindowUpdate(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		blockResetTransport(t, stream.session, conn)
		payload := bytes.Repeat([]byte("x"), int(initialStreamWindow/2))
		stream.recvBuf = bytes.NewBuffer(payload)
		stream.recvWindow -= uint32(len(payload))
		var n int
		read := resetAsync(func() error {
			var err error
			n, err = stream.Read(make([]byte, len(payload)))
			return err
		})
		synctest.Wait() // Read has copied bytes and queued its window update.
		reset := resetAsync(stream.Reset)
		synctest.Wait()
		if err := awaitSendResult(t, read); err != ErrConnectionReset || n != len(payload) {
			t.Fatalf("overlapping Read = %d, %v", n, err)
		}
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		assertRST(t, awaitControlledWrite(t, conn), stream.id)
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		if err := awaitSendResult(t, reset); err != nil {
			t.Fatal(err)
		}
		assertResetIO(t, stream)
		resetWireBarrier(t, stream.session, conn)
	})
}

func TestStreamResetSendClaim(t *testing.T) {
	// Force the send loop's claim path to observe Reset before the sender's
	// cancellation select runs. Rollback must happen once and no body may leak.
	abort := make(chan struct{})
	calls := 0
	h := header(make([]byte, headerSize))
	r := newSendReadyWithHooks(h, []byte("unsent"), func() { calls++ })
	r.abortCh = abort
	close(abort)
	if body, ok := r.claim(); ok || body != nil {
		t.Fatal("reset request committed")
	}
	if err := <-r.done; err != ErrConnectionReset {
		t.Fatalf("claim result = %v", err)
	}
	if r.cancel() {
		t.Fatal("request canceled twice")
	}
	if _, ok := r.claim(); ok {
		t.Fatal("request claimed twice")
	}
	if calls != 1 || r.Body != nil {
		t.Fatalf("rollback count=%d body=%q", calls, r.Body)
	}
}

func resetWireSession(t *testing.T) (*Session, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	cfg := testConfNoKeepAlive()
	cfg.LogOutput = io.Discard
	cfg.StreamOpenTimeout = 0
	cfg.StreamCloseTimeout = 0
	s, err := Client(local, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close(); _ = s.Close() })
	return s, peer
}

func readResetWireHeader(t *testing.T, peer io.Reader) header {
	t.Helper()
	h := header(make([]byte, headerSize))
	if _, err := io.ReadFull(peer, h); err != nil {
		t.Fatal(err)
	}
	return h
}

func writeResetWireFrame(t *testing.T, peer io.Writer, typ uint8, flags uint16, id uint32, payload []byte) {
	t.Helper()
	h := header(make([]byte, headerSize))
	h.encode(typ, flags, id, uint32(len(payload)))
	if err := writeFrame(peer, append(h, payload...)); err != nil {
		t.Fatal(err)
	}
}

func openResetWireStream(t *testing.T, s *Session, peer net.Conn, ack bool) *Stream {
	t.Helper()
	var stream *Stream
	opened := resetAsync(func() error {
		var err error
		stream, err = s.OpenStream()
		return err
	})
	h := readResetWireHeader(t, peer)
	if h.Version() != 0 || h.MsgType() != typeWindowUpdate || h.Flags() != flagSYN {
		t.Fatalf("SYN = %v", h)
	}
	if err := awaitSendResult(t, opened); err != nil {
		t.Fatal(err)
	}
	if ack {
		writeResetWireFrame(t, peer, typeWindowUpdate, flagACK, stream.id, nil)
		synctest.Wait()
	}
	return stream
}

func TestStreamResetVersionZeroWire(t *testing.T) {
	for _, ack := range []bool{false, true} {
		name := "before ACK"
		if ack {
			name = "established"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				s, peer := resetWireSession(t)
				stream := openResetWireStream(t, s, peer, ack)
				writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("unread"))
				synctest.Wait()
				if stream.recvBuf == nil || stream.recvBuf.String() != "unread" {
					t.Fatal("payload not buffered before Reset")
				}
				reset := resetAsync(stream.Reset)
				// Exact pre-existing version-0 encoding, with no ACK, FIN, new
				// flags, payload, or reset acknowledgement exchange.
				want := []byte{0, 1, 0, 8, 0, 0, 0, 1, 0, 0, 0, 0}
				if got := readResetWireHeader(t, peer); !bytes.Equal(got, want) {
					t.Fatalf("wire RST = %v", got)
				}
				if err := awaitSendResult(t, reset); err != nil {
					t.Fatal(err)
				}
				assertResetIO(t, stream)
				if s.NumStreams() != 0 || len(s.inflight) != 0 || len(s.synCh) != 0 {
					t.Fatal("reset leaked stream or SYN credit")
				}

				// An old peer may have already sent DATA, ACK, FIN, or RST.
				// Drain the data and ignore late control frames without closing
				// the session or returning SYN credit twice.
				writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("late data"))
				for _, flag := range []uint16{flagACK, flagFIN, flagRST} {
					writeResetWireFrame(t, peer, typeWindowUpdate, flag, stream.id, nil)
				}
				next := openResetWireStream(t, s, peer, true)
				writeResetWireFrame(t, peer, typeData, 0, next.id, []byte("next"))
				buf := make([]byte, 4)
				if _, err := io.ReadFull(next, buf); err != nil || string(buf) != "next" {
					t.Fatalf("next stream = %q, %v", buf, err)
				}
				if s.IsClosed() {
					t.Fatal("RST closed the session")
				}
			})
		})
	}
}

func TestStreamResetPeerRST(t *testing.T) {
	for _, typ := range []uint8{typeWindowUpdate, typeData} {
		name := "window update"
		if typ == typeData {
			name = "data with payload"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				s, peer := resetWireSession(t)
				stream := openResetWireStream(t, s, peer, true)
				writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("discard me"))
				var payload []byte
				if typ == typeData {
					payload = []byte("RST payload")
				}
				writeResetWireFrame(t, peer, typ, flagRST, stream.id, payload)
				synctest.Wait()
				assertResetIO(t, stream)
				if err := stream.Reset(); err != nil {
					t.Fatal(err)
				}
				// Peer RST must not be echoed. The next outbound frame is SYN.
				_ = openResetWireStream(t, s, peer, true)
			})
		})
	}
}

func TestStreamResetDuringDataFrame(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		s, peer := resetWireSession(t)
		stream := openResetWireStream(t, s, peer, true)
		writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("buffered"))
		h := header(make([]byte, headerSize))
		h.encode(typeData, 0, stream.id, 7)
		if err := writeFrame(peer, append(h, []byte("par")...)); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // recvLoop is blocked waiting for the second half.
		reset := resetAsync(stream.Reset)
		assertRST(t, readResetWireHeader(t, peer), stream.id)
		if err := awaitSendResult(t, reset); err != nil {
			t.Fatal(err)
		}
		assertResetIO(t, stream)
		if err := writeFrame(peer, []byte("tial")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		assertResetIO(t, stream)
		next := openResetWireStream(t, s, peer, true)
		writeResetWireFrame(t, peer, typeData, 0, next.id, []byte("ok"))
		buf := make([]byte, 2)
		if _, err := io.ReadFull(next, buf); err != nil || string(buf) != "ok" {
			t.Fatalf("next frame lost: %q, %v", buf, err)
		}
	})
}

func TestStreamResetDataFINOrdering(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		s, peer := resetWireSession(t)
		stream := openResetWireStream(t, s, peer, true)
		h := header(make([]byte, headerSize))
		h.encode(typeData, flagFIN, stream.id, 4)
		if err := writeFrame(peer, append(h, []byte("fi")...)); err != nil {
			t.Fatal(err)
		}
		var got []byte
		read := resetAsync(func() error {
			var err error
			got, err = io.ReadAll(stream)
			return err
		})
		synctest.Wait()
		select {
		case err := <-read:
			t.Fatalf("EOF before FIN payload: %q, %v", got, err)
		default:
		}
		if err := writeFrame(peer, []byte("ni")); err != nil {
			t.Fatal(err)
		}
		if err := awaitSendResult(t, read); err != nil || string(got) != "fini" {
			t.Fatalf("FIN payload: %q, %v", got, err)
		}
	})
}

func TestStreamResetTruncatedData(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "active"
		if reset {
			name = "reset"
		}
		t.Run(name, func(t *testing.T) {
			stream := newStream(newSendTestSession(newControlledWriteConn(), time.Second), 1, streamEstablished)
			if reset {
				if err := stream.processFlags(flagRST); err != nil {
					t.Fatal(err)
				}
			}
			h := header(make([]byte, headerSize))
			h.encode(typeData, 0, stream.id, 4)
			err := stream.readData(h, 0, bytes.NewBufferString("abc"))
			if err != io.ErrUnexpectedEOF && err != io.EOF {
				t.Fatalf("truncated frame = %v", err)
			}
		})
	}
}

func TestStreamResetBeforeAccept(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		s, peer := resetWireSession(t)
		writeResetWireFrame(t, peer, typeWindowUpdate, flagSYN, 2, nil)
		writeResetWireFrame(t, peer, typeWindowUpdate, flagRST, 2, nil)
		synctest.Wait()
		stream, err := s.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		assertResetIO(t, stream)
		if err := stream.Reset(); err != nil {
			t.Fatal(err)
		}
		if s.NumStreams() != 0 {
			t.Fatal("accept resurrected a reset stream")
		}
		_ = openResetWireStream(t, s, peer, true)
	})
}

func TestStreamResetConcurrentACKCleanup(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		s := stream.session
		stream.state = streamSYNSent
		stream.openInFlight = true
		s.inflight[stream.id] = struct{}{}
		s.synCh <- struct{}{}
		// Pause ACK after it publishes established state but before it can
		// return the SYN credit. Reset must race that exact cleanup boundary.
		s.streamLock.Lock()
		ack := resetAsync(func() error { return stream.processFlags(flagACK) })
		<-stream.establishCh
		reset := resetAsync(stream.Reset)
		<-stream.resetCh
		s.streamLock.Unlock()
		if err := awaitSendResult(t, ack); err != nil {
			t.Fatal(err)
		}
		assertRST(t, awaitControlledWrite(t, conn), stream.id)
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		if err := awaitSendResult(t, reset); err != nil {
			t.Fatal(err)
		}
		if s.NumStreams() != 0 || len(s.inflight) != 0 || len(s.synCh) != 0 {
			t.Fatal("ACK/reset cleanup leaked SYN credit")
		}
		assertResetIO(t, stream)
		resetWireBarrier(t, s, conn)
	})
}

func TestStreamResetCommittedFINFailure(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		firstClose := resetAsync(stream.Close)
		if frame := awaitControlledWrite(t, conn); header(frame).Flags() != flagFIN {
			t.Fatalf("FIN: %v", frame)
		}
		secondClose := resetAsync(stream.Close)
		synctest.Wait()
		reset := resetAsync(stream.Reset)
		synctest.Wait()
		transportErr := errors.New("committed FIN failed")
		releaseControlledWrite(t, conn, controlledWriteResult{err: transportErr})
		for _, result := range []<-chan error{firstClose, secondClose} {
			if err := awaitSendResult(t, result); err != transportErr {
				t.Fatalf("Close = %v", err)
			}
		}
		if err := awaitSendResult(t, reset); err != ErrSessionShutdown {
			t.Fatalf("unsent RST = %v", err)
		}
		if err := stream.Close(); err != transportErr {
			t.Fatalf("retained Close = %v", err)
		}
		if err := stream.Reset(); err != ErrSessionShutdown {
			t.Fatalf("retained Reset = %v", err)
		}
		assertResetIO(t, stream)
	})
}

func TestStreamResetBufferedReadShutdown(t *testing.T) {
	for _, reset := range []bool{false, true} {
		name := "transport shutdown preserves data"
		if reset {
			name = "reset error survives shutdown"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				payload := bytes.Repeat([]byte("x"), int(initialStreamWindow/2))
				stream.recvBuf = bytes.NewBuffer(payload)
				stream.recvWindow -= uint32(len(payload))
				if reset {
					blockResetTransport(t, stream.session, conn)
				}
				var n int
				read := resetAsync(func() error {
					var err error
					n, err = stream.Read(make([]byte, len(payload)))
					return err
				})
				want := error(nil)
				if reset {
					synctest.Wait()
					// Latch shutdown before canceling the queued window update.
					// This error must not mask the local Reset's cancellation.
					stream.session.recordShutdownErr(io.EOF)
					resetResult := resetAsync(stream.Reset)
					synctest.Wait()
					want = ErrConnectionReset
					if err := stream.session.Close(); err != nil {
						t.Fatal(err)
					}
					if err := awaitSendResult(t, resetResult); err != ErrSessionShutdown {
						t.Fatal(err)
					}
				} else {
					if frame := awaitControlledWrite(t, conn); header(frame).MsgType() != typeWindowUpdate {
						t.Fatalf("update: %v", frame)
					}
					releaseControlledWrite(t, conn, controlledWriteResult{err: net.ErrClosed})
				}
				if err := awaitSendResult(t, read); err != want || n != len(payload) {
					t.Fatalf("buffered Read = %d, %v", n, err)
				}
				if reset {
					assertResetIO(t, stream)
				}
			})
		})
	}
}
