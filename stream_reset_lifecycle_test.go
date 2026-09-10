// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestStreamResetPendingOwnership(t *testing.T) {
	for _, outcome := range []string{"success", "queue timeout", "shutdown", "transport failure"} {
		t.Run(outcome, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, conn := newResetTestStream(t)
				s := stream.session
				queued := outcome == "queue timeout" || outcome == "shutdown"
				if queued {
					blockResetTransport(t, s, conn)
				}
				result := resetAsync(stream.Reset)
				synctest.Wait()
				if !queued {
					assertRST(t, awaitControlledWrite(t, conn), stream.id)
				}
				s.streamLock.Lock()
				pending := s.pendingReset[stream.id]
				s.streamLock.Unlock()
				if pending == nil || s.NumStreams() != 0 {
					t.Fatal("Reset did not reserve the removed stream ID")
				}
				// A stale timer cannot steal public Reset's marker or send another RST.
				stream.closeTimeout()
				if err := s.incomingStream(stream.id); err != nil {
					t.Fatal(err)
				}
				if s.NumStreams() != 0 || len(s.acceptCh) != 0 {
					t.Fatal("same-ID SYN was accepted while RST was pending")
				}
				unrelated := addIncomingTestStream(t, s, 3)
				<-s.acceptCh
				h := header(make([]byte, headerSize))
				h.encode(typeWindowUpdate, 0, unrelated.id, 7)
				if err := s.handleStreamMessage(h); err != nil {
					t.Fatal(err)
				}
				if atomic.LoadUint32(&unrelated.sendWindow) != initialStreamWindow+7 {
					t.Fatal("pending Reset blocked unrelated stream processing")
				}
				want := error(nil)
				switch outcome {
				case "success":
					releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				case "queue timeout":
					want = ErrConnectionWriteTimeout
					time.Sleep(s.config.ConnectionWriteTimeout)
				case "shutdown":
					want = ErrSessionShutdown
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
				case "transport failure":
					want = errors.New("RST transport failed")
					releaseControlledWrite(t, conn, controlledWriteResult{err: want})
				}
				if err := awaitSendResult(t, result); err != want {
					t.Fatalf("Reset = %v, want %v", err, want)
				}
				select {
				case <-pending:
				default:
					t.Fatal("Reset returned before releasing its ID reservation")
				}
				s.streamLock.Lock()
				left := len(s.pendingReset)
				s.streamLock.Unlock()
				if left != 0 {
					t.Fatal("Reset leaked pending ownership")
				}
				if outcome == "queue timeout" {
					releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				}
				if outcome == "success" || outcome == "queue timeout" {
					if err := s.incomingStream(stream.id); err != nil {
						t.Fatal(err)
					}
					s.streamLock.Lock()
					replacement := s.streams[stream.id]
					s.streamLock.Unlock()
					if replacement == nil || replacement == stream {
						t.Fatal("ID reservation remained after Reset completed")
					}
					if err := stream.Reset(); err != want {
						t.Fatalf("repeated Reset = %v", err)
					}
					stream.closeTimeout()
					resetWireBarrier(t, s, conn)
				}
				assertResetIO(t, stream)
			})
		})
	}
}

func TestStreamResetStaleOwnership(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		replacement, conn := newResetTestStream(t)
		stale := newStream(replacement.session, replacement.id, streamEstablished)
		if err := stale.Reset(); err != nil {
			t.Fatal(err)
		}
		assertResetIO(t, stale)
		stale.closeTimeout()
		s := replacement.session
		s.streamLock.Lock()
		owned := s.streams[replacement.id] == replacement
		pending := len(s.pendingReset)
		s.streamLock.Unlock()
		if !owned || pending != 0 {
			t.Fatal("stale Reset affected replacement ownership")
		}
		resetWireBarrier(t, s, conn)
	})
}

func TestStreamResetAfterTimeout(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		stream.recvBuf = bytes.NewBufferString("buffered")
		stream.closeTimeout() // Must return without waiting for the transport.
		if err := stream.Reset(); err != nil {
			t.Fatal(err)
		}
		assertRST(t, awaitControlledWrite(t, conn), stream.id)
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		buf := make([]byte, 8)
		if n, err := stream.Read(buf); n != 8 || err != nil || string(buf) != "buffered" {
			t.Fatalf("timeout changed reviewed buffered-read behavior: %d, %v", n, err)
		}
		if _, err := stream.Read(buf); err != io.EOF {
			t.Fatalf("Read after timeout = %v", err)
		}
		if _, err := stream.Write(buf); err != ErrStreamClosed {
			t.Fatalf("Write after timeout = %v", err)
		}
		resetWireBarrier(t, stream.session, conn)
	})
}

func TestStreamResetSelectedAccept(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		name := "AcceptStream"
		if withContext {
			name = "AcceptStreamWithContext"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				owner, conn := newResetTestStream(t)
				s := owner.session
				s.closeStreamIfOwned(owner)
				stream := addIncomingTestStream(t, s, 2)
				blockResetTransport(t, s, conn)
				accepted := resetAsync(func() error {
					var got *Stream
					var err error
					if withContext {
						got, err = s.AcceptStreamWithContext(context.Background())
					} else {
						got, err = s.AcceptStream()
					}
					if got != nil {
						t.Error("canceled ACK returned a stream")
					}
					return err
				})
				synctest.Wait() // Accept selected the stream and queued its ACK.
				reset := resetAsync(stream.Reset)
				synctest.Wait()
				if err := awaitSendResult(t, accepted); err != ErrConnectionReset {
					t.Fatalf("Accept = %v", err)
				}
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				assertRST(t, awaitControlledWrite(t, conn), stream.id)
				releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
				if err := awaitSendResult(t, reset); err != nil {
					t.Fatal(err)
				}
				if s.NumStreams() != 0 || len(s.acceptCh) != 0 || len(s.pendingReset) != 0 {
					t.Fatal("selected Accept cleanup resurrected Reset ownership")
				}
				assertResetIO(t, stream)
				resetWireBarrier(t, s, conn)
			})
		})
	}
}

func TestStreamResetSelectedAcceptDuringSessionClose(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		owner, _ := newResetTestStream(t)
		s := owner.session
		stream := addIncomingTestStream(t, s, 2)
		<-s.acceptCh // Accept has selected this pointer before shutdown starts.
		s.recvDoneCh = make(chan struct{})
		closing := resetAsync(s.Close)
		synctest.Wait() // Session.Close holds shutdownLock at receive teardown.
		accepted := resetAsync(func() error {
			got, err := s.acceptStream(stream)
			if got != nil {
				t.Error("Accept returned a stream after Session.Close")
			}
			return err
		})
		if err := stream.Reset(); err != ErrSessionShutdown {
			t.Fatalf("Reset = %v", err)
		}
		close(s.recvDoneCh)
		if err := awaitSendResult(t, closing); err != nil {
			t.Fatal(err)
		}
		if err := awaitSendResult(t, accepted); err != ErrSessionShutdown {
			t.Fatalf("Accept = %v", err)
		}
		assertResetIO(t, stream)
		if s.NumStreams() != 0 || len(s.acceptCh) != 0 || len(s.pendingReset) != 0 {
			t.Fatal("Session.Close/Accept/Reset leaked ownership")
		}
	})
}

func TestStreamResetDrainPreservesReaderError(t *testing.T) {
	stream := newStream(newSendTestSession(nil, time.Second), 1, streamEstablished)
	if err := stream.processFlags(flagRST); err != nil {
		t.Fatal(err)
	}
	transportErr := errors.New("last DATA read failed")
	h := header(make([]byte, headerSize))
	h.encode(typeData, 0, stream.id, 4)
	err := stream.readData(h, 0, &terminalErrorReader{data: []byte("body"), err: transportErr})
	if err != transportErr {
		t.Fatalf("reset DATA drain = %v", err)
	}
	assertResetIO(t, stream)
}
