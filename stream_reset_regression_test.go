// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
)

func TestStreamResetBufferedBeforeAccept(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		name := "AcceptStream"
		if withContext {
			name = "AcceptStreamWithContext"
		}
		t.Run(name, func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				local, peer := net.Pipe()
				s, err := Client(local, nil) // Exercise the unmodified default configuration.
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = peer.Close(); _ = s.Close() })
				accept := s.AcceptStream
				if withContext {
					accept = func() (*Stream, error) { return s.AcceptStreamWithContext(context.Background()) }
				}

				writeResetWireFrame(t, peer, typeWindowUpdate, flagSYN, 2, nil)
				payload := bytes.Repeat([]byte("x"), 128*1024)
				writeResetWireFrame(t, peer, typeData, 0, 2, payload)
				synctest.Wait()
				s.streamLock.Lock()
				pending := s.streams[2]
				s.streamLock.Unlock()
				pending.recvLock.Lock()
				buffered, window := pending.recvBuf.Len(), pending.recvWindow
				pending.recvLock.Unlock()
				if len(s.acceptCh) != 1 || buffered != len(payload) || window != initialStreamWindow-uint32(len(payload)) {
					t.Fatalf("before RST: queued=%d buffered=%d window=%d", len(s.acceptCh), buffered, window)
				}
				writeResetWireFrame(t, peer, typeWindowUpdate, flagRST, 2, nil)
				synctest.Wait()
				assertResetIO(t, pending)

				var stream *Stream
				accepted := resetAsync(func() error {
					var err error
					stream, err = accept()
					return err
				})
				synctest.Wait() // The peer is not reading: any transport write stays blocked.
				select {
				case err := <-accepted:
					if err != nil || stream != pending {
						t.Fatalf("Accept = %p, %v; want terminal stream %p", stream, err, pending)
					}
				default:
					t.Fatal("Accept blocked on a window update after buffered peer RST")
				}
				assertResetIO(t, stream)
				if s.NumStreams() != 0 || len(s.acceptCh) != 0 {
					t.Fatal("Accept resurrected a remotely reset stream")
				}

				// The next outbound SYN is also a wire barrier for stray updates.
				_ = openResetWireStream(t, s, peer, true)
				// A normal incoming stream must still send its ACK through either API.
				writeResetWireFrame(t, peer, typeWindowUpdate, flagSYN, 4, nil)
				writeResetWireFrame(t, peer, typeData, 0, 4, []byte("ok"))
				accepted = resetAsync(func() error {
					var err error
					stream, err = accept()
					return err
				})
				if h := readResetWireHeader(t, peer); h.MsgType() != typeWindowUpdate || h.Flags() != flagACK || h.StreamID() != 4 || h.Length() != 0 {
					t.Fatalf("normal ACK = %v", h)
				}
				if err := awaitSendResult(t, accepted); err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, 2)
				if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "ok" {
					t.Fatalf("normal accepted Read = %q, %v", buf, err)
				}
			})
		})
	}
}

// Hold a transport Read error until Close so the inner session deterministically
// discovers the outer stream reset through Write. Successful reads are unchanged.
type resetReadErrorBarrier struct {
	*Stream
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *resetReadErrorBarrier) Read(p []byte) (int, error) {
	n, err := c.Stream.Read(p)
	if err != nil {
		<-c.closed
	}
	return n, err
}

func (c *resetReadErrorBarrier) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Stream.Close()
}

func TestStreamReadBufferedDataAfterNestedTransportReset(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		local, peer := net.Pipe()
		cfg := testConfNoKeepAlive()
		cfg.LogOutput = io.Discard
		outer, err := Client(local, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = outer.Close() })
		outerPeer, err := Server(peer, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = outerPeer.Close() })
		transport, err := outer.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		peerTransport, err := outerPeer.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		inner, err := Client(&resetReadErrorBarrier{Stream: transport, closed: make(chan struct{})}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = inner.Close() })
		innerPeer, err := Server(peerTransport, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = innerPeer.Close() })
		writer, err := innerPeer.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		reader, err := inner.AcceptStream()
		if err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte("0123456789abcdef"), 262144/16)
		if n, err := writer.Write(payload); n != len(payload) || err != nil {
			t.Fatalf("nested Write = %d, %v", n, err)
		}
		synctest.Wait()
		reader.recvLock.Lock()
		buffered, window := reader.recvBuf.Len(), reader.recvWindow
		reader.recvLock.Unlock()
		if buffered != len(payload) || window != 0 {
			t.Fatalf("before outer reset: buffered=%d window=%d", buffered, window)
		}
		if err := peerTransport.Reset(); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		assertResetIO(t, transport)
		if inner.isShuttingDown() {
			t.Fatal("inner session shut down before its window update reached transport.Write")
		}

		got, err := io.ReadAll(reader)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("ReadAll after outer RST = %d/%d bytes, %v", len(got), len(payload), err)
		}
		if inner.shutdownError() != ErrConnectionReset {
			t.Fatalf("inner transport failure = %v", inner.shutdownError())
		}
		reader.stateLock.Lock()
		state, locallyReset := reader.state, reader.resetResult != nil
		reader.stateLock.Unlock()
		if state != streamClosed || locallyReset {
			t.Fatalf("outer RST reset the inner stream: state=%v local reset=%v", state, locallyReset)
		}
	})
}

func TestStreamResetCommittedReadTransportError(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		stream, conn := newResetTestStream(t)
		payload := bytes.Repeat([]byte("x"), int(initialStreamWindow/2))
		stream.recvBuf = bytes.NewBuffer(payload)
		stream.recvWindow -= uint32(len(payload))
		buf := make([]byte, len(payload))
		var n int
		read := resetAsync(func() error {
			var err error
			n, err = stream.Read(buf)
			return err
		})
		if h := header(awaitControlledWrite(t, conn)); h.MsgType() != typeWindowUpdate || h.StreamID() != stream.id {
			t.Fatalf("window update = %v", h)
		}
		reset := resetAsync(stream.Reset)
		synctest.Wait() // Reset cannot cancel the already committed window update.
		releaseControlledWrite(t, conn, controlledWriteResult{err: ErrConnectionReset})
		if err := awaitSendResult(t, read); err != nil || n != len(payload) || !bytes.Equal(buf, payload) {
			t.Fatalf("committed Read = %d, %v; want transferred bytes without transport error", n, err)
		}
		if err := awaitSendResult(t, reset); err != ErrSessionShutdown {
			t.Fatalf("queued RST after transport failure = %v", err)
		}
		assertResetIO(t, stream)
	})
}
