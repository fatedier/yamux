// Copyright IBM Corp. 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// Isolate receive allocations with unread data retained between DATA frames.
// Network, send requests, and application buffer allocation are excluded.
func BenchmarkStreamReceiveBuffered(b *testing.B) {
	for _, size := range []int{32 * 1024, 128 * 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			s := newStream(newSendTestSession(nil, time.Second), 1, streamEstablished)
			s.recvBuf = bytes.NewBuffer(make([]byte, 1, 4*size))
			h := header(make([]byte, headerSize))
			h.encode(typeData, 0, s.id, uint32(size))
			payload := make([]byte, size)
			reader := bytes.NewReader(payload)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.Reset(payload)
				if err := s.readData(h, 0, reader); err != nil {
					b.Fatal(err)
				}
				s.recvBuf.Next(size)
				s.recvWindow += uint32(size)
			}
		})
	}
}
