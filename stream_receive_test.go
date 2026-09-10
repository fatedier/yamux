// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"
)

func TestStreamReceiveCloseWaitsForPartialFrame(t *testing.T) {
	for _, mode := range []string{"timeout", "force close"} {
		for _, buffered := range []string{"", "buffered"} {
			t.Run(mode+"/"+buffered, func(t *testing.T) {
				resetSynctest(t, func(t *testing.T) {
					s, peer := resetWireSession(t)
					s.config.StreamCloseTimeout = time.Second
					stream := openResetWireStream(t, s, peer, true)
					if mode == "timeout" {
						closing := resetAsync(stream.Close)
						if h := readResetWireHeader(t, peer); h.Flags() != flagFIN {
							t.Fatalf("FIN: %v", h)
						}
						if err := awaitSendResult(t, closing); err != nil {
							t.Fatal(err)
						}
					}
					writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte(buffered))
					h := header(make([]byte, headerSize))
					h.encode(typeData, 0, stream.id, 7)
					if err := writeFrame(peer, append(h, []byte("par")...)); err != nil {
						t.Fatal(err)
					}
					var got []byte
					read := resetAsync(func() error {
						var err error
						got, err = io.ReadAll(stream)
						return err
					})
					synctest.Wait() // DATA has only partially arrived; Read has drained published bytes.
					if mode == "timeout" {
						time.Sleep(time.Second)
						assertRST(t, readResetWireHeader(t, peer), stream.id)
					} else {
						stream.forceClose()
					}
					synctest.Wait()
					select {
					case err := <-read:
						t.Fatalf("ReadAll returned before the DATA tail: %q, %v", got, err)
					default:
					}
					if err := writeFrame(peer, []byte("tial")); err != nil {
						t.Fatal(err)
					}
					if err := awaitSendResult(t, read); err != nil || string(got) != buffered+"partial" {
						t.Fatalf("ReadAll lost DATA: %q, %v", got, err)
					}
					// A frame dispatched after the terminal transition cannot make
					// data readable after EOF, even if its stream pointer was retained.
					writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("late"))
					synctest.Wait()
					if n, err := stream.Read(make([]byte, 8)); n != 0 || err != io.EOF {
						t.Fatalf("Read after EOF: %d, %v", n, err)
					}
					next := openResetWireStream(t, s, peer, true)
					writeResetWireFrame(t, peer, typeData, 0, next.id, []byte("ok"))
					buf := make([]byte, 2)
					if _, err := io.ReadFull(next, buf); err != nil || string(buf) != "ok" {
						t.Fatalf("framing lost: %q, %v", buf, err)
					}
				})
			})
		}
	}
}

func TestStreamReceiveCloseWaitsForFailedFrame(t *testing.T) {
	transportErr := errors.New("partial DATA transport failed")
	for _, failure := range []error{io.EOF, transportErr} {
		t.Run(failure.Error(), func(t *testing.T) {
			resetSynctest(t, func(t *testing.T) {
				stream, _ := newResetTestStream(t)
				reader, writer := io.Pipe()
				defer func() { _ = reader.Close(); _ = writer.Close() }()
				h := header(make([]byte, headerSize))
				h.encode(typeData, 0, stream.id, 7)
				received := resetAsync(func() error { return stream.readData(h, 0, reader) })
				if _, err := writer.Write([]byte("par")); err != nil {
					t.Fatal(err)
				}
				stream.forceClose()
				var got []byte
				read := resetAsync(func() error {
					var err error
					got, err = io.ReadAll(stream)
					return err
				})
				synctest.Wait()
				select {
				case err := <-read:
					t.Fatalf("EOF before the partial frame finished: %q, %v", got, err)
				default:
				}
				if err := writer.CloseWithError(failure); err != nil {
					t.Fatal(err)
				}
				want := failure
				if failure == io.EOF {
					want = io.ErrUnexpectedEOF
				}
				if err := awaitSendResult(t, received); err != want {
					t.Fatalf("readData = %v, want %v", err, want)
				}
				if err := awaitSendResult(t, read); err != nil || string(got) != "par" {
					t.Fatalf("partial bytes lost: %q, %v", got, err)
				}
			})
		})
	}
}

func TestStreamReceiveReadAndShrinkDuringFrame(t *testing.T) {
	resetSynctest(t, func(t *testing.T) {
		s, peer := resetWireSession(t)
		stream := openResetWireStream(t, s, peer, true)
		writeResetWireFrame(t, peer, typeData, 0, stream.id, []byte("old"))
		h := header(make([]byte, headerSize))
		h.encode(typeData, 0, stream.id, 7)
		if err := writeFrame(peer, append(h, []byte("par")...)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 3)
		if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "old" {
			t.Fatalf("published data: %q, %v", buf, err)
		}
		stream.Shrink()
		if err := writeFrame(peer, []byte("tial")); err != nil {
			t.Fatal(err)
		}
		buf = make([]byte, 7)
		if _, err := io.ReadFull(stream, buf); err != nil || string(buf) != "partial" {
			t.Fatalf("reserved data: %q, %v", buf, err)
		}
		stream.Shrink()
		stream.recvLock.Lock()
		retained := stream.recvBuf != nil
		stream.recvLock.Unlock()
		if retained {
			t.Fatal("Shrink retained idle receive storage")
		}
	})
}

func TestStreamReceiveBufferedAllocations(t *testing.T) {
	for _, size := range []int{32 * 1024, 128 * 1024} {
		payload := make([]byte, size)
		s := newStream(newSendTestSession(nil, time.Second), 1, streamEstablished)
		s.recvBuf = bytes.NewBuffer(make([]byte, 1, 4*size))
		h := header(make([]byte, headerSize))
		h.encode(typeData, 0, s.id, uint32(size))
		reader := bytes.NewReader(payload)
		allocs := testing.AllocsPerRun(100, func() {
			reader.Reset(payload)
			if err := s.readData(h, 0, reader); err != nil {
				t.Fatal(err)
			}
			// Model a consumer that always leaves one byte buffered, then
			// returns flow-control credit. No network or sender allocation.
			s.recvBuf.Next(size)
			s.recvWindow += uint32(size)
		})
		if allocs != 0 {
			t.Fatalf("%d-byte buffered DATA allocated %g objects/frame, want 0", size, allocs)
		}
	}
}
