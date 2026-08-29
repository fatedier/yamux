// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type logCapture struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

var _ io.Writer = (*logCapture)(nil)

func (l *logCapture) Write(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.buf == nil {
		l.buf = &bytes.Buffer{}
	}
	return l.buf.Write(p)
}
func (l *logCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logCapture) logs() []string {
	return strings.Split(strings.TrimSpace(l.String()), "\n")
}

func (l *logCapture) match(expect []string) bool {
	return reflect.DeepEqual(l.logs(), expect)
}

type pipeConn struct {
	reader       *io.PipeReader
	writer       *io.PipeWriter
	writeBlocker sync.Mutex
}

func (p *pipeConn) Read(b []byte) (int, error) {
	return p.reader.Read(b)
}

func (p *pipeConn) Write(b []byte) (int, error) {
	p.writeBlocker.Lock()
	defer p.writeBlocker.Unlock()
	return p.writer.Write(b)
}

func (p *pipeConn) Close() error {
	_ = p.reader.Close()
	return p.writer.Close()
}

func testConnPipe(testing.TB) (io.ReadWriteCloser, io.ReadWriteCloser) {
	read1, write1 := io.Pipe()
	read2, write2 := io.Pipe()
	conn1 := &pipeConn{reader: read1, writer: write2}
	conn2 := &pipeConn{reader: read2, writer: write1}
	return conn1, conn2
}

func testConnTCP(t testing.TB) (io.ReadWriteCloser, io.ReadWriteCloser) {
	l, err := net.ListenTCP("tcp", nil)
	if err != nil {
		t.Fatalf("error creating listener: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	network := l.Addr().Network()
	addr := l.Addr().String()

	var server net.Conn
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		var err error
		server, err = l.Accept()
		if err != nil {
			errCh <- err
			return
		}
	}()

	t.Logf("Connecting to %s: %s", network, addr)
	client, err := net.DialTimeout(network, addr, 10*time.Second)
	if err != nil {
		t.Fatalf("error dialing tls listener: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := <-errCh; err != nil {
		t.Fatalf("error creating tls server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	return client, server
}

func testConnTLS(t testing.TB) (io.ReadWriteCloser, io.ReadWriteCloser) {
	cert, err := tls.LoadX509KeyPair("testdata/cert.pem", "testdata/key.pem")
	if err != nil {
		t.Fatalf("error loading certificate: %v", err)
	}

	l, err := net.ListenTCP("tcp", nil)
	if err != nil {
		t.Fatalf("error creating listener: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	var server net.Conn
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		conn, err := l.Accept()
		if err != nil {
			errCh <- err
			return
		}

		server = tls.Server(conn, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}()

	t.Logf("Connecting to %s: %s", l.Addr().Network(), l.Addr())
	client, err := net.DialTimeout(l.Addr().Network(), l.Addr().String(), 10*time.Second)
	if err != nil {
		t.Fatalf("error dialing tls listener: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	tlsClient := tls.Client(client, &tls.Config{
		// InsecureSkipVerify is safe to use here since this is only for tests.
		InsecureSkipVerify: true,
	})

	if err := <-errCh; err != nil {
		t.Fatalf("error creating tls server: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	return tlsClient, server
}

// connTypeFunc is func that returns a client and server connection for testing
// like testConnTLS.
//
// See connTypeTest
type connTypeFunc func(t testing.TB) (io.ReadWriteCloser, io.ReadWriteCloser)

// connTypeTest is a test case for a specific conn type.
//
// See testConnType
type connTypeTest struct {
	Name  string
	Conns connTypeFunc
}

// testConnType runs subtests of the given testFunc against multiple connection
// types.
func testConnTypes(t *testing.T, testFunc func(t testing.TB, client, server io.ReadWriteCloser)) {
	reverse := func(f connTypeFunc) connTypeFunc {
		return func(t testing.TB) (io.ReadWriteCloser, io.ReadWriteCloser) {
			c, s := f(t)
			return s, c
		}
	}
	cases := []connTypeTest{
		{
			Name:  "Pipes",
			Conns: testConnPipe,
		},
		{
			Name:  "TCP",
			Conns: testConnTCP,
		},
		{
			Name:  "TCP_Reverse",
			Conns: reverse(testConnTCP),
		},
		{
			Name:  "TLS",
			Conns: testConnTLS,
		},
		{
			Name:  "TLS_Reverse",
			Conns: reverse(testConnTLS),
		},
	}
	for i := range cases {
		tc := cases[i]
		t.Run(tc.Name, func(t *testing.T) {
			client, server := tc.Conns(t)
			testFunc(t, client, server)
		})
	}
}

func testConf() *Config {
	conf := DefaultConfig()
	conf.AcceptBacklog = 64
	conf.KeepAliveInterval = 100 * time.Millisecond
	conf.ConnectionWriteTimeout = 250 * time.Millisecond
	return conf
}

func captureLogs(conf *Config) *logCapture {
	buf := new(logCapture)
	conf.Logger = log.New(buf, "", 0)
	conf.LogOutput = nil
	return buf
}

func testConfNoKeepAlive() *Config {
	conf := testConf()
	conf.EnableKeepAlive = false
	return conf
}

func testClientServer(t testing.TB) (*Session, *Session) {
	client, server := testConnTLS(t)
	return testClientServerConfig(t, client, server, testConf(), testConf())
}

func testClientServerConfig(
	t testing.TB,
	clientConn, serverConn io.ReadWriteCloser,
	clientConf, serverConf *Config,
) (clientSession *Session, serverSession *Session) {

	var err error

	clientSession, err = Client(clientConn, clientConf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = clientSession.Close() })

	serverSession, err = Server(serverConn, serverConf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	return clientSession, serverSession
}

func TestPing(t *testing.T) {
	client, server := testClientServer(t)

	rtt, err := client.Ping()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if rtt == 0 {
		t.Fatalf("bad: %v", rtt)
	}

	rtt, err = server.Ping()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if rtt == 0 {
		t.Fatalf("bad: %v", rtt)
	}
}

func TestPing_Timeout(t *testing.T) {
	conf := testConfNoKeepAlive()
	clientPipe, serverPipe := testConnPipe(t)
	client, server := testClientServerConfig(t, clientPipe, serverPipe, conf.Clone(), conf.Clone())

	// Prevent the client from responding
	clientConn := client.conn.(*pipeConn)
	clientConn.writeBlocker.Lock()

	errCh := make(chan error, 1)
	go func() {
		_, err := server.Ping() // Ping via the server session
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != ErrTimeout {
			t.Fatalf("err: %v", err)
		}
	case <-time.After(client.config.ConnectionWriteTimeout * 2):
		t.Fatalf("failed to timeout within expected %v", client.config.ConnectionWriteTimeout)
	}

	// Verify that we recover, even if we gave up
	clientConn.writeBlocker.Unlock()

	go func() {
		_, err := server.Ping() // Ping via the server session
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	case <-time.After(client.config.ConnectionWriteTimeout):
		t.Fatalf("timeout")
	}
}

func TestCloseBeforeAck(t *testing.T) {
	testConnTypes(t, func(t testing.TB, clientConn, serverConn io.ReadWriteCloser) {
		cfg := testConf()
		cfg.AcceptBacklog = 8
		client, server := testClientServerConfig(t, clientConn, serverConn, cfg, cfg.Clone())

		for i := 0; i < 8; i++ {
			s, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			_ = s.Close()
		}

		for i := 0; i < 8; i++ {
			s, err := server.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			_ = s.Close()
		}

		errCh := make(chan error, 1)
		go func() {
			s, err := client.OpenStream()
			if err != nil {
				errCh <- err
				return
			}
			_ = s.Close()
			errCh <- nil
		}()

		drainErrorsUntil(t, errCh, 1, time.Second*5, "timed out trying to open stream")
	})
}

func TestAccept(t *testing.T) {
	client, server := testClientServer(t)

	if client.NumStreams() != 0 {
		t.Fatalf("bad")
	}
	if server.NumStreams() != 0 {
		t.Fatalf("bad")
	}

	errCh := make(chan error, 4)
	acceptOne := func(streamFunc func() (*Stream, error), expectID uint32) {
		stream, err := streamFunc()
		if err != nil {
			errCh <- err
			return
		}
		if id := stream.StreamID(); id != expectID {
			errCh <- fmt.Errorf("bad: %v", id)
			return
		}
		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}

	go acceptOne(server.AcceptStream, 1)
	go acceptOne(client.AcceptStream, 2)
	go acceptOne(server.OpenStream, 2)
	go acceptOne(client.OpenStream, 1)

	drainErrorsUntil(t, errCh, 4, time.Second, "timeout")
}

func TestOpenStreamTimeout(t *testing.T) {
	const timeout = 25 * time.Millisecond

	testConnTypes(t, func(t testing.TB, clientConn, serverConn io.ReadWriteCloser) {
		serverConf := testConf()
		serverConf.StreamOpenTimeout = timeout

		clientConf := serverConf.Clone()
		clientLogs := captureLogs(clientConf)

		client, _ := testClientServerConfig(t, clientConn, serverConn, clientConf, serverConf)

		// Open a single stream without a server to acknowledge it.
		s, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}

		// Sleep for longer than the stream open timeout.
		// Since no ACKs are received, the stream and session should be closed.
		time.Sleep(timeout * 5)

		// Support multiple underlying connection types
		var dest string
		switch conn := clientConn.(type) {
		case net.Conn:
			dest = conn.RemoteAddr().String()
		case *pipeConn:
			dest = "yamux:remote"
		default:
			t.Fatalf("unsupported connection type %T - please update test", conn)
		}
		exp := fmt.Sprintf("[ERR] yamux: aborted stream open (destination=%s): i/o deadline reached", dest)

		if !clientLogs.match([]string{exp}) {
			t.Fatalf("server log incorect: %v\nexpected: %v", clientLogs.logs(), exp)
		}

		s.stateLock.Lock()
		state := s.state
		s.stateLock.Unlock()

		if state != streamClosed {
			t.Fatalf("stream should have been closed")
		}
		if !client.IsClosed() {
			t.Fatalf("session should have been closed")
		}
	})
}

func TestClose_closeTimeout(t *testing.T) {
	conf := testConf()
	conf.StreamCloseTimeout = 10 * time.Millisecond
	clientConn, serverConn := testConnTLS(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	if client.NumStreams() != 0 {
		t.Fatalf("bad")
	}
	if server.NumStreams() != 0 {
		t.Fatalf("bad")
	}

	errCh := make(chan error, 2)

	// Open a stream on the client but only close it on the server.
	// We want to see if the stream ever gets cleaned up on the client.

	var clientStream *Stream
	go func() {
		var err error
		clientStream, err = client.OpenStream()
		errCh <- err
	}()

	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, time.Second, "timeout")

	// We should have zero streams after our timeout period
	time.Sleep(100 * time.Millisecond)

	if v := server.NumStreams(); v > 0 {
		t.Fatalf("should have zero streams: %d", v)
	}
	if v := client.NumStreams(); v > 0 {
		t.Fatalf("should have zero streams: %d", v)
	}

	if _, err := clientStream.Write([]byte("hello")); err == nil {
		t.Fatal("should error on write")
	} else if err.Error() != "connection reset" {
		t.Fatalf("expected connection reset, got %q", err)
	}
}

func TestNonNilInterface(t *testing.T) {
	_, server := testClientServer(t)
	_ = server.Close()

	conn, err := server.Accept()
	if err == nil || !errors.Is(err, ErrSessionShutdown) || conn != nil {
		t.Fatal("bad: accept should return a shutdown error and a connection of nil value")
	}
	if err != nil && conn != nil {
		t.Error("bad: accept should return a connection of nil value")
	}

	conn, err = server.Open()
	if err == nil || !errors.Is(err, ErrSessionShutdown) || conn != nil {
		t.Fatal("bad: open should return a shutdown error and a connection of nil value")
	}
}

func TestSendData_Small(t *testing.T) {
	client, server := testClientServer(t)

	errCh := make(chan error, 2)

	// Accept an incoming client and perform some reads before closing
	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}

		if server.NumStreams() != 1 {
			errCh <- fmt.Errorf("bad")
			return
		}

		buf := make([]byte, 4)
		for i := 0; i < 1000; i++ {
			n, err := stream.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short read: %d", n)
				return
			}
			if string(buf) != "test" {
				errCh <- fmt.Errorf("bad: %s", buf)
				return
			}
		}

		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// Open a client and perform some writes before closing
	go func() {
		stream, err := client.Open()
		if err != nil {
			errCh <- err
			return
		}

		if client.NumStreams() != 1 {
			errCh <- fmt.Errorf("bad")
			return
		}

		for i := 0; i < 1000; i++ {
			n, err := stream.Write([]byte("test"))
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short write %d", n)
				return
			}
		}

		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 5*time.Second, "timeout")

	// Give client and server a second to receive FINs and close streams
	time.Sleep(time.Second)

	if n := client.NumStreams(); n != 0 {
		t.Errorf("expected 0 client streams but found %d", n)
	}
	if n := server.NumStreams(); n != 0 {
		t.Errorf("expected 0 server streams but found %d", n)
	}
}

func TestSendData_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test that may time out on the race detector")
	}
	client, server := testClientServer(t)

	const (
		sendSize = 250 * 1024 * 1024
		recvSize = 4 * 1024
	)

	data := make([]byte, sendSize)
	for idx := range data {
		data[idx] = byte(idx % 256)
	}

	errCh := make(chan error, 2)

	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		var sz int
		buf := make([]byte, recvSize)
		for i := 0; i < sendSize/recvSize; i++ {
			n, err := stream.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if n != recvSize {
				errCh <- fmt.Errorf("short read: %d", n)
				return
			}
			sz += n
			for idx := range buf {
				if buf[idx] != byte(idx%256) {
					errCh <- fmt.Errorf("bad: %v %v %v", i, idx, buf[idx])
					return
				}
			}
		}

		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}

		t.Logf("cap=%d, n=%d\n", stream.recvBuf.Cap(), sz)
		errCh <- nil
	}()

	go func() {
		stream, err := client.Open()
		if err != nil {
			errCh <- err
			return
		}

		n, err := stream.Write(data)
		if err != nil {
			errCh <- err
			return
		}
		if n != len(data) {
			errCh <- fmt.Errorf("short write %d", n)
			return
		}

		if err := stream.Close(); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 10*time.Second, "timeout")
}

func TestGoAway(t *testing.T) {
	client, server := testClientServer(t)

	if err := server.GoAway(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Give the other side time to process the goaway after receiving it.
	time.Sleep(100 * time.Millisecond)

	_, err := client.Open()
	if err != ErrRemoteGoAway {
		t.Fatalf("err: %v", err)
	}
}

func TestManyStreams(t *testing.T) {
	client, server := testClientServer(t)

	const streams = 50

	errCh := make(chan error, 2*streams)

	acceptor := func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		buf := make([]byte, 512)
		for {
			n, err := stream.Read(buf)
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			if n == 0 {
				errCh <- fmt.Errorf("no bytes read")
				return
			}
		}
	}
	sender := func(id int) {
		stream, err := client.Open()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		msg := fmt.Sprintf("%08d", id)
		for i := 0; i < 1000; i++ {
			n, err := stream.Write([]byte(msg))
			if err != nil {
				errCh <- err
				return
			}
			if n != len(msg) {
				errCh <- fmt.Errorf("short write %d", n)
				return
			}
		}
		errCh <- nil
	}

	for i := 0; i < streams; i++ {
		go acceptor()
		go sender(i)
	}

	drainErrorsUntil(t, errCh, 2*streams, 0, "")
}

func TestManyStreams_PingPong(t *testing.T) {
	client, server := testClientServer(t)

	const streams = 50

	errCh := make(chan error, 2*streams)

	ping := []byte("ping")
	pong := []byte("pong")

	acceptor := func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		buf := make([]byte, 4)
		for {
			// Read the 'ping'
			n, err := stream.Read(buf)
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short read %d", n)
				return
			}
			if !bytes.Equal(buf, ping) {
				errCh <- fmt.Errorf("bad: %s", buf)
				return
			}

			// Shrink the internal buffer!
			stream.Shrink()

			// Write out the 'pong'
			n, err = stream.Write(pong)
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short write %d", n)
				return
			}
		}
	}
	sender := func() {
		stream, err := client.OpenStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		buf := make([]byte, 4)
		for i := 0; i < 1000; i++ {
			// Send the 'ping'
			n, err := stream.Write(ping)
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short write %d", n)
				return
			}

			// Read the 'pong'
			n, err = stream.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if n != 4 {
				errCh <- fmt.Errorf("short read %d", n)
				return
			}
			if !bytes.Equal(buf, pong) {
				errCh <- fmt.Errorf("bad: %s", buf)
				return
			}

			// Shrink the buffer
			stream.Shrink()
		}
		errCh <- nil
	}

	for i := 0; i < streams; i++ {
		go acceptor()
		go sender()
	}

	drainErrorsUntil(t, errCh, 2*streams, 0, "")
}

// TestHalfClose asserts that half closed streams can still read.
func TestHalfClose(t *testing.T) {
	testConnTypes(t, func(t testing.TB, clientConn, serverConn io.ReadWriteCloser) {
		client, server := testClientServerConfig(t, clientConn, serverConn, testConf(), testConf())

		clientStream, err := client.Open()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if _, err = clientStream.Write([]byte("a")); err != nil {
			t.Fatalf("err: %v", err)
		}

		serverStream, err := server.Accept()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		_ = serverStream.Close() // Half close

		// Server reads 1 byte written by Client
		buf := make([]byte, 4)
		n, err := serverStream.Read(buf)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if n != 1 {
			t.Fatalf("bad: %v", n)
		}

		// Send more
		if _, err = clientStream.Write([]byte("bcd")); err != nil {
			t.Fatalf("err: %v", err)
		}
		_ = clientStream.Close()

		// Read after close always returns the bytes written but may or may not
		// receive the EOF.
		n, err = serverStream.Read(buf)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if n != 3 {
			t.Fatalf("bad: %v", n)
		}

		// EOF after close
		n, err = serverStream.Read(buf)
		if err != io.EOF {
			t.Fatalf("err: %v", err)
		}
		if n != 0 {
			t.Fatalf("bad: %v", n)
		}
	})
}

func TestHalfCloseSessionShutdown(t *testing.T) {
	client, server := testClientServer(t)

	// dataSize must be large enough to ensure the server will send a window
	// update
	dataSize := int64(server.config.MaxStreamWindowSize)

	data := make([]byte, dataSize)
	for idx := range data {
		data[idx] = byte(idx % 256)
	}

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, err = stream.Write(data); err != nil {
		t.Fatalf("err: %v", err)
	}

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Shut down the session of the sending side. This should not cause reads
	// to fail on the receiving side.
	if err := client.Close(); err != nil {
		t.Fatalf("err: %v", err)
	}

	buf := make([]byte, dataSize)
	n, err := stream2.Read(buf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if int64(n) != dataSize {
		t.Fatalf("bad: %v", n)
	}

	// EOF after close
	n, err = stream2.Read(buf)
	if err != io.EOF {
		t.Fatalf("err: %v", err)
	}
	if n != 0 {
		t.Fatalf("bad: %v", n)
	}
}

func TestReadDeadline(t *testing.T) {
	client, server := testClientServer(t)

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream.Close() }()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	if err := stream.SetReadDeadline(time.Now().Add(5 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	buf := make([]byte, 4)
	_, err = stream.Read(buf)
	if err != ErrTimeout {
		t.Fatalf("err: %v", err)
	}

	// See https://github.com/hashicorp/yamux/issues/90
	// The standard library's http server package will read from connections in
	// the background to detect if they are alive.
	//
	// It sets a read deadline on connections and detect if the returned error
	// is a network timeout error which implements net.Error.
	//
	// The HTTP server will cancel all server requests if it isn't timeout error
	// from the connection.
	//
	// We assert that we return an error meeting the interface to avoid
	// accidently breaking yamux session compatability with the standard
	// library's http server implementation.
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("reading timeout error must implement net.Error with Timeout() == true for HTTP server compatibility")
	}

	type frpTLSTemporaryError interface {
		Temporary() bool
	}
	if temporaryErr, ok := err.(frpTLSTemporaryError); !ok || !temporaryErr.Temporary() {
		t.Fatalf("reading timeout error must implement Temporary() bool with Temporary() == true for FRP/TLS compatibility")
	}
}

func TestReadDeadline_BlockedRead(t *testing.T) {
	client, server := testClientServer(t)

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream.Close() }()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	// Start a read that will block
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 4)
		_, err := stream.Read(buf)
		errCh <- err
		close(errCh)
	}()

	// Wait to ensure the read has started.
	time.Sleep(5 * time.Millisecond)

	// Update the read deadline
	if err := stream.SetReadDeadline(time.Now().Add(5 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected read timeout")
	case err := <-errCh:
		if err != ErrTimeout {
			t.Fatalf("expected ErrTimeout; got %v", err)
		}
	}
}

func TestWriteDeadline(t *testing.T) {
	client, server := testClientServer(t)

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream.Close() }()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	if err := stream.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	buf := make([]byte, 512)
	for i := 0; i < int(initialStreamWindow); i++ {
		_, err := stream.Write(buf)
		if err != nil && err == ErrTimeout {
			return
		} else if err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	t.Fatalf("Expected timeout")
}

func TestWriteDeadline_BlockedWrite(t *testing.T) {
	client, server := testClientServer(t)

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream.Close() }()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	// Start a goroutine making writes that will block
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 512)
		for i := 0; i < int(initialStreamWindow); i++ {
			_, err := stream.Write(buf)
			if err == nil {
				continue
			}

			errCh <- err
			close(errCh)
			return
		}

		close(errCh)
	}()

	// Wait to ensure the write has started.
	time.Sleep(5 * time.Millisecond)

	// Update the write deadline
	if err := stream.SetWriteDeadline(time.Now().Add(5 * time.Millisecond)); err != nil {
		t.Fatalf("err: %v", err)
	}

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("expected write timeout")
	case err := <-errCh:
		if err != ErrTimeout {
			t.Fatalf("expected ErrTimeout; got %v", err)
		}
	}
}

func TestBacklogExceeded(t *testing.T) {
	client, server := testClientServer(t)

	// Fill the backlog
	max := client.config.AcceptBacklog
	for i := 0; i < max; i++ {
		stream, err := client.Open()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = stream.Close() }()

		if _, err := stream.Write([]byte("foo")); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	// Attempt to open a new stream
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Open()
		errCh <- err
	}()

	// Shutdown the server
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = server.Close()
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("open should fail")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
}

func TestKeepAlive(t *testing.T) {
	testConnTypes(t, func(t testing.TB, clientConn, serverConn io.ReadWriteCloser) {
		client, server := testClientServerConfig(t, clientConn, serverConn, testConf(), testConf())

		// Give keepalives time to happen
		time.Sleep(200 * time.Millisecond)

		// Ping value should increase
		client.pingLock.Lock()
		defer client.pingLock.Unlock()
		if client.pingID == 0 {
			t.Fatalf("should ping")
		}

		server.pingLock.Lock()
		defer server.pingLock.Unlock()
		if server.pingID == 0 {
			t.Fatalf("should ping")
		}
	})
}

func TestKeepAlive_Timeout(t *testing.T) {
	conn1, conn2 := testConnPipe(t)

	clientConf := testConf()
	clientConf.ConnectionWriteTimeout = time.Hour // We're testing keep alives, not connection writes
	clientConf.EnableKeepAlive = false            // Just test one direction, so it's deterministic who hangs up on whom
	_ = captureLogs(clientConf)                   // Client logs aren't part of the test
	client, err := Client(conn1, clientConf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = client.Close() }()

	serverConf := testConf()
	serverLogs := captureLogs(serverConf)
	server, err := Server(conn2, serverConf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = server.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := server.Accept() // Wait until server closes
		errCh <- err
	}()

	// Prevent the client from responding
	clientConn := client.conn.(*pipeConn)
	clientConn.writeBlocker.Lock()

	select {
	case err := <-errCh:
		if err != ErrKeepAliveTimeout {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for timeout")
	}

	clientConn.writeBlocker.Unlock()

	if !server.IsClosed() {
		t.Fatalf("server should have closed")
	}

	if !serverLogs.match([]string{"[ERR] yamux: keepalive failed: i/o deadline reached"}) {
		t.Fatalf("server log incorect: %v", serverLogs.logs())
	}
}

func TestLargeWindow(t *testing.T) {
	conf := DefaultConfig()
	conf.MaxStreamWindowSize *= 2

	clientConn, serverConn := testConnTLS(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	stream, err := client.Open()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream.Close() }()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	err = stream.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	buf := make([]byte, conf.MaxStreamWindowSize)
	n, err := stream.Write(buf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("short write: %d", n)
	}
}

type UnlimitedReader struct{}

func (u *UnlimitedReader) Read(p []byte) (int, error) {
	runtime.Gosched()
	return len(p), nil
}

func TestSendData_VeryLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test that may time out on the race detector")
	}
	client, server := testClientServer(t)

	var n int64 = 1 * 1024 * 1024 * 1024
	var workers = 16

	errCh := make(chan error, workers*2)

	for i := 0; i < workers; i++ {
		go func() {
			stream, err := server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = stream.Close() }()

			buf := make([]byte, 4)
			_, err = stream.Read(buf)
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(buf, []byte{0, 1, 2, 3}) {
				errCh <- errors.New("bad header")
				return
			}

			recv, err := io.Copy(io.Discard, stream)
			if err != nil {
				errCh <- err
				return
			}
			if recv != n {
				errCh <- fmt.Errorf("bad: %v", recv)
				return
			}

			errCh <- nil
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			stream, err := client.Open()
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = stream.Close() }()

			_, err = stream.Write([]byte{0, 1, 2, 3})
			if err != nil {
				errCh <- err
				return
			}

			unlimited := &UnlimitedReader{}
			sent, err := io.Copy(stream, io.LimitReader(unlimited, n))
			if err != nil {
				errCh <- err
				return
			}
			if sent != n {
				errCh <- fmt.Errorf("bad: %v", sent)
				return
			}

			errCh <- nil
		}()
	}

	drainErrorsUntil(t, errCh, workers*2, 120*time.Second, "timeout")
}

func TestBacklogExceeded_Accept(t *testing.T) {
	client, server := testClientServer(t)

	max := 5 * client.config.AcceptBacklog

	errCh := make(chan error, max)
	go func() {
		for i := 0; i < max; i++ {
			stream, err := server.Accept()
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = stream.Close() }()
			errCh <- nil
		}
	}()

	// Fill the backlog
	for i := 0; i < max; i++ {
		stream, err := client.Open()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = stream.Close() }()

		if _, err := stream.Write([]byte("foo")); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	drainErrorsUntil(t, errCh, max, 0, "")
}

func TestSession_WindowUpdateWriteDuringRead(t *testing.T) {
	conf := testConfNoKeepAlive()

	clientConn, serverConn := testConnPipe(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	// Choose a huge flood size that we know will result in a window update.
	flood := int64(client.config.MaxStreamWindowSize) - 1

	errCh := make(chan error, 2)

	// The server will accept a new stream and then flood data to it.
	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		n, err := stream.Write(make([]byte, flood))
		if err != nil {
			errCh <- err
			return
		}
		if int64(n) != flood {
			errCh <- fmt.Errorf("short write: %d", n)
		}

		errCh <- nil
	}()

	// The client will open a stream, block outbound writes, and then
	// listen to the flood from the server. The window update may commit
	// before the transport write completes, so the read must wait for the
	// committed request instead of reporting an unsent timeout.
	go func() {
		stream, err := client.OpenStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		conn := clientConn.(*pipeConn)
		conn.writeBlocker.Lock()
		unlocked := false
		defer func() {
			if !unlocked {
				conn.writeBlocker.Unlock()
			}
		}()
		readErrCh := make(chan error, 1)
		go func() {
			_, readErr := stream.Read(make([]byte, flood))
			readErrCh <- readErr
		}()

		select {
		case err := <-readErrCh:
			errCh <- fmt.Errorf("committed window update returned before transport release: %v", err)
			return
		case <-time.After(2 * client.config.ConnectionWriteTimeout):
		}
		conn.writeBlocker.Unlock()
		unlocked = true

		select {
		case err := <-readErrCh:
			if err != nil {
				errCh <- err
				return
			}
		case <-time.After(time.Second):
			errCh <- fmt.Errorf("timed out waiting for committed window update")
			return
		}

		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 0, "")
}

// TestSession_PartialReadWindowUpdate asserts that when a client performs a
// partial read it updates the server's send window.
func TestSession_PartialReadWindowUpdate(t *testing.T) {
	testConnTypes(t, func(t testing.TB, clientConn, serverConn io.ReadWriteCloser) {
		conf := testConfNoKeepAlive()

		client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

		errCh := make(chan error, 1)

		// Choose a huge flood size that we know will result in a window update.
		flood := int64(client.config.MaxStreamWindowSize)
		var wr *Stream

		// The server will accept a new stream and then flood data to it.
		go func() {
			var err error
			wr, err = server.AcceptStream()
			if err != nil {
				errCh <- err
				return
			}
			defer func() { _ = wr.Close() }()

			window := atomic.LoadUint32(&wr.sendWindow)
			if window != client.config.MaxStreamWindowSize {
				errCh <- fmt.Errorf("sendWindow: exp=%d, got=%d", client.config.MaxStreamWindowSize, window)
				return
			}

			n, err := wr.Write(make([]byte, flood))
			if err != nil {
				errCh <- err
				return
			}
			if int64(n) != flood {
				errCh <- fmt.Errorf("short write: %d", n)
				return
			}
			window = atomic.LoadUint32(&wr.sendWindow)
			if window != 0 {
				errCh <- fmt.Errorf("sendWindow: exp=%d, got=%d", 0, window)
				return
			}
			errCh <- err
		}()

		stream, err := client.OpenStream()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer func() { _ = stream.Close() }()

		drainErrorsUntil(t, errCh, 1, 0, "")

		// Only read part of the flood
		partialReadSize := flood/2 + 1
		_, err = stream.Read(make([]byte, partialReadSize))
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		// Wait for window update to be applied by server. Should be "instant" but CI
		// can be slow.
		time.Sleep(2 * time.Second)

		// Assert server received window update
		window := atomic.LoadUint32(&wr.sendWindow)
		if exp := uint32(partialReadSize); window != exp {
			t.Fatalf("sendWindow: exp=%d, got=%d", exp, window)
		}
	})
}

func TestSession_sendNoWait_Timeout(t *testing.T) {
	conf := testConfNoKeepAlive()

	clientConn, serverConn := testConnPipe(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	errCh := make(chan error, 2)

	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()
		errCh <- nil
	}()

	// The client will open the stream and then block outbound writes, we'll
	// probe sendNoWait once it gets into that state.
	go func() {
		stream, err := client.OpenStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		conn := clientConn.(*pipeConn)
		conn.writeBlocker.Lock()
		defer conn.writeBlocker.Unlock()

		hdr := header(make([]byte, headerSize))
		hdr.encode(typePing, flagACK, 0, 0)
		for {
			err = client.sendNoWait(hdr)
			if err == nil {
				continue
			} else if err == ErrConnectionWriteTimeout {
				break
			} else {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 0, "")
}

func TestSession_PingOfDeath(t *testing.T) {
	conf := testConfNoKeepAlive()

	clientConn, serverConn := testConnPipe(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	errCh := make(chan error, 2)

	var doPingOfDeath sync.Mutex
	doPingOfDeath.Lock()

	// This is used later to block outbound writes.
	conn := server.conn.(*pipeConn)

	// The server will accept a stream, block outbound writes, and then
	// flood its send channel so that no more headers can be queued.
	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		conn.writeBlocker.Lock()
		for {
			hdr := header(make([]byte, headerSize))
			hdr.encode(typePing, 0, 0, 0)
			err = server.sendNoWait(hdr)
			if err == nil {
				continue
			} else if err == ErrConnectionWriteTimeout {
				break
			} else {
				errCh <- err
				return
			}
		}

		doPingOfDeath.Unlock()
		errCh <- nil
	}()

	// The client will open a stream and then send the server a ping once it
	// can no longer write. This makes sure the server doesn't deadlock reads
	// while trying to reply to the ping with no ability to write.
	go func() {
		stream, err := client.OpenStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		// This ping will never unblock because the ping id will never
		// show up in a response.
		doPingOfDeath.Lock()
		go func() { _, _ = client.Ping() }()

		// Wait for a while to make sure the previous ping times out,
		// then turn writes back on and make sure a ping works again.
		time.Sleep(2 * server.config.ConnectionWriteTimeout)
		conn.writeBlocker.Unlock()
		if _, err = client.Ping(); err != nil {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 0, "")
}

func TestSession_ConnectionWriteTimeout(t *testing.T) {
	conf := testConfNoKeepAlive()

	clientConn, serverConn := testConnPipe(t)
	client, server := testClientServerConfig(t, clientConn, serverConn, conf, conf.Clone())

	errCh := make(chan error, 2)

	go func() {
		stream, err := server.AcceptStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()
		errCh <- nil
	}()

	// The client opens a stream, blocks the underlying transport write, and
	// verifies that a committed request does not turn into a synthetic timeout.
	go func() {
		stream, err := client.OpenStream()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = stream.Close() }()

		conn := clientConn.(*pipeConn)
		conn.writeBlocker.Lock()
		unlocked := false
		defer func() {
			if !unlocked {
				conn.writeBlocker.Unlock()
			}
		}()

		type writeResult struct {
			n   int
			err error
		}
		resultCh := make(chan writeResult, 1)
		go func() {
			n, err := stream.Write([]byte("hello"))
			resultCh <- writeResult{n: n, err: err}
		}()

		select {
		case result := <-resultCh:
			errCh <- fmt.Errorf("committed write returned before transport release: n=%d err=%v", result.n, result.err)
			return
		case <-time.After(2 * client.config.ConnectionWriteTimeout):
		}

		conn.writeBlocker.Unlock()
		unlocked = true
		select {
		case result := <-resultCh:
			if result.n != len("hello") || result.err != nil {
				errCh <- fmt.Errorf("committed write result: n=%d err=%v", result.n, result.err)
				return
			}
		case <-time.After(time.Second):
			errCh <- fmt.Errorf("timed out waiting for committed write completion")
			return
		}

		errCh <- nil
	}()

	drainErrorsUntil(t, errCh, 2, 5*time.Second, "connection write timeout test")
}

type controlledWriteResult struct {
	n   int
	err error
}

type controlledWriteConn struct {
	writes    chan []byte
	results   chan controlledWriteResult
	closed    chan struct{}
	closeOnce sync.Once
}

type fragmentedReader struct {
	data []byte
	step int
}

func (r *fragmentedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := minInt(len(p), r.step)
	n = minInt(n, len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type terminalErrorReader struct {
	data []byte
	err  error
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}

type scriptedReader struct {
	chunks [][]byte
	errs   []error
	reads  int
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if r.reads >= len(r.chunks) {
		return 0, io.EOF
	}
	chunk := r.chunks[r.reads]
	err := r.errs[r.reads]
	r.reads++
	if len(chunk) > len(p) {
		panic("scripted reader chunk exceeds read buffer")
	}
	copy(p, chunk)
	return len(chunk), err
}

func newControlledWriteConn() *controlledWriteConn {
	return &controlledWriteConn{
		writes:  make(chan []byte),
		results: make(chan controlledWriteResult),
		closed:  make(chan struct{}),
	}
}

func (c *controlledWriteConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *controlledWriteConn) Write(p []byte) (int, error) {
	buf := append([]byte(nil), p...)
	select {
	case c.writes <- buf:
	case <-c.closed:
		return 0, net.ErrClosed
	}

	select {
	case result := <-c.results:
		return result.n, result.err
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *controlledWriteConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func newSendTestSession(conn io.ReadWriteCloser, timeout time.Duration) *Session {
	return &Session{
		config: &Config{
			ConnectionWriteTimeout: timeout,
			MaxStreamWindowSize:    initialStreamWindow,
		},
		logger:     log.New(io.Discard, "", 0),
		conn:       conn,
		sendCh:     make(chan *sendReady, 8),
		sendDoneCh: make(chan struct{}),
		shutdownCh: make(chan struct{}),
	}
}

func newOpenTestSession(conn io.ReadWriteCloser, timeout time.Duration) *Session {
	s := newSendTestSession(conn, timeout)
	s.streams = make(map[uint32]*Stream)
	s.inflight = make(map[uint32]struct{})
	s.pendingReset = make(map[uint32]<-chan struct{})
	s.synCh = make(chan struct{}, 1)
	s.acceptCh = make(chan *Stream, 1)
	s.recvDoneCh = make(chan struct{})
	return s
}

func prepareSessionClose(t *testing.T, session *Session) {
	t.Helper()
	// The focused sessions do not run recvLoop; make Session.Close's receive
	// completion barrier deterministic without starting another goroutine.
	close(session.recvDoneCh)
}

func addIncomingTestStream(t *testing.T, session *Session, id uint32) *Stream {
	t.Helper()
	if err := session.incomingStream(id); err != nil {
		t.Fatalf("incoming stream %d: %v", id, err)
	}
	session.streamLock.Lock()
	stream := session.streams[id]
	session.streamLock.Unlock()
	if stream == nil {
		t.Fatalf("incoming stream %d was not registered", id)
	}
	return stream
}

func waitForAcceptSelection(t *testing.T, session *Session) {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for len(session.acceptCh) != 0 {
		select {
		case <-timeout.C:
			t.Fatal("timed out waiting for AcceptStream to select queued stream")
		default:
			runtime.Gosched()
		}
	}
}

func startSendLoop(s *Session) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.sendLoop()
	}()
	return errCh
}

func awaitControlledWrite(t *testing.T, conn *controlledWriteConn) []byte {
	t.Helper()
	select {
	case p := <-conn.writes:
		return p
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transport write")
		return nil
	}
}

func releaseControlledWrite(t *testing.T, conn *controlledWriteConn, result controlledWriteResult) {
	t.Helper()
	select {
	case conn.results <- result:
	case <-time.After(time.Second):
		t.Fatal("timed out releasing transport write")
	}
}

func awaitSendResult(t *testing.T, errCh <-chan error) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for send result")
		return nil
	}
}

func stopSendLoop(t *testing.T, s *Session, errCh <-chan error) {
	t.Helper()
	close(s.shutdownCh)
	if err := awaitSendResult(t, errCh); err != nil {
		t.Fatalf("send loop exited with error: %v", err)
	}
}

func TestSession_QueuedStreamWriteTimeoutCancellation(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, 20*time.Millisecond)
	stream := newStream(session, 1, streamEstablished)

	type writeResult struct {
		n   int
		err error
	}
	canceledResultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.Write([]byte("canceled"))
		canceledResultCh <- writeResult{n: n, err: err}
	}()

	select {
	case result := <-canceledResultCh:
		if result.n != 0 || result.err != ErrConnectionWriteTimeout {
			t.Fatalf("queued stream write result: n=%d err=%v", result.n, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued stream write cancellation")
	}

	// Start the sender only after the first request is canceled. The next frame
	// must be the first frame placed on the wire; the canceled frame must be
	// skipped and cannot leave a completion for this request to consume.
	session.config.ConnectionWriteTimeout = time.Second
	sendLoopErrCh := startSendLoop(session)

	nextResultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.Write([]byte("next"))
		nextResultCh <- writeResult{n: n, err: err}
	}()

	writtenHeader := awaitControlledWrite(t, conn)
	if header(writtenHeader).MsgType() != typeData || header(writtenHeader).Length() != 4 {
		t.Fatalf("unexpected next stream header: %v", header(writtenHeader))
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(writtenHeader)})
	writtenBody := awaitControlledWrite(t, conn)
	if !bytes.Equal(writtenBody, []byte("next")) {
		t.Fatalf("unexpected next stream body: %q", writtenBody)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(writtenBody)})
	select {
	case result := <-nextResultCh:
		if result.n != 4 || result.err != nil {
			t.Fatalf("next stream write result: n=%d err=%v", result.n, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for next stream write")
	}

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestSession_CommittedWriteWaitsForCompletion(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, 20*time.Millisecond)
	sendLoopErrCh := startSendLoop(session)
	stream := newStream(session, 1, streamEstablished)

	type writeResult struct {
		n   int
		err error
	}
	resultCh := make(chan writeResult, 1)
	payload := []byte("committed")
	go func() {
		n, err := stream.Write(payload)
		resultCh <- writeResult{n: n, err: err}
	}()

	writtenHeader := awaitControlledWrite(t, conn)
	if header(writtenHeader).MsgType() != typeData || header(writtenHeader).Length() != uint32(len(payload)) {
		t.Fatalf("unexpected data header: %v", header(writtenHeader))
	}

	// Reaching the transport Write means sendLoop has committed the request.
	// The queue timeout must no longer report that the request was unsent.
	select {
	case result := <-resultCh:
		t.Fatalf("committed write returned before transport completion: n=%d err=%v", result.n, result.err)
	case <-time.After(3 * session.config.ConnectionWriteTimeout):
	}

	releaseControlledWrite(t, conn, controlledWriteResult{n: len(writtenHeader)})
	writtenBody := awaitControlledWrite(t, conn)
	if !bytes.Equal(writtenBody, payload) {
		t.Fatalf("unexpected data body: got %q, want %q", writtenBody, payload)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(writtenBody)})

	select {
	case result := <-resultCh:
		if result.err != nil || result.n != len(payload) {
			t.Fatalf("committed write result: n=%d err=%v", result.n, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed write completion")
	}
	if window := atomic.LoadUint32(&stream.sendWindow); window != initialStreamWindow-uint32(len(payload)) {
		t.Fatalf("send window updated at wrong point: got %d", window)
	}

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestStreamCloseTimeoutDoesNotWaitForCommittedWrite(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := newStream(session, 1, streamEstablished)
	session.streamLock.Lock()
	session.streams[stream.id] = stream
	session.inflight[stream.id] = struct{}{}
	session.synCh <- struct{}{}
	session.streamLock.Unlock()

	sendLoopErrCh := startSendLoop(session)
	type writeResult struct {
		n   int
		err error
	}
	writeResultCh := make(chan writeResult, 1)
	go func() {
		n, err := stream.Write([]byte("blocked"))
		writeResultCh <- writeResult{n: n, err: err}
	}()
	// sendLoop has claimed the request once its header reaches Write. The
	// Stream.Write caller still owns sendLock until this transport completes.
	frame := awaitControlledWrite(t, conn)
	if hdr := header(frame); hdr.MsgType() != typeData || hdr.StreamID() != stream.id {
		t.Fatalf("unexpected committed DATA: %v", hdr)
	}

	closeTimeoutDone := make(chan struct{})
	go func() {
		stream.closeTimeout()
		close(closeTimeoutDone)
	}()
	select {
	case <-closeTimeoutDone:
	case <-time.After(time.Second):
		// Unblock the committed write before failing so the test cannot leak a
		// sender goroutine when run against the old implementation.
		_ = conn.Close()
		t.Fatal("closeTimeout waited for committed transport write")
	}

	if got := session.NumStreams(); got != 0 {
		t.Fatalf("stream registry after timeout cleanup: got %d, want 0", got)
	}
	session.streamLock.Lock()
	inflight := len(session.inflight)
	session.streamLock.Unlock()
	if inflight != 0 {
		t.Fatalf("inflight registry after timeout cleanup: got %d, want 0", inflight)
	}
	assertSynCreditAvailable(t, session)

	transportErr := errors.New("committed write released after timeout cleanup")
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(frame), err: transportErr})
	result := <-writeResultCh
	if result.n != 0 || !errors.Is(result.err, transportErr) {
		t.Fatalf("committed write result: n=%d err=%v", result.n, result.err)
	}
	if err := awaitSendResult(t, sendLoopErrCh); !errors.Is(err, transportErr) {
		t.Fatalf("send loop result: got %v, want %v", err, transportErr)
	}
}

func TestStreamCloseTimeoutSaturatedQueueDoesNotBlockUnrelatedStream(t *testing.T) {
	session := newOpenTestSession(nil, time.Hour)
	session.sendCh = make(chan *sendReady, 1)
	blocker := header(make([]byte, headerSize))
	blocker.encode(typePing, flagACK, 0, 1)
	session.sendCh <- newSendReady(blocker)

	timedOut := newStream(session, 1, streamEstablished)
	unrelated := newStream(session, 3, streamEstablished)
	session.streamLock.Lock()
	session.streams[timedOut.id] = timedOut
	session.streams[unrelated.id] = unrelated
	session.streamLock.Unlock()

	closeDone := make(chan struct{})
	go func() {
		timedOut.closeTimeout()
		close(closeDone)
	}()

	// terminal is published before closeTimeout attempts RST admission. This is
	// the deterministic barrier that the timeout path has started.
	started := make(chan struct{})
	go func() {
		for {
			timedOut.stateLock.Lock()
			terminal := timedOut.terminal
			timedOut.stateLock.Unlock()
			if terminal {
				close(started)
				return
			}
			runtime.Gosched()
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("closeTimeout did not publish terminal state")
	}

	hdr := header(make([]byte, headerSize))
	hdr.encode(typeWindowUpdate, 0, unrelated.id, 1)
	unrelatedDone := make(chan error, 1)
	go func() { unrelatedDone <- session.handleStreamMessage(hdr) }()

	select {
	case err := <-unrelatedDone:
		if err != nil {
			t.Fatalf("unrelated WINDOW_UPDATE: %v", err)
		}
	case <-time.After(time.Second):
		// Free capacity so an implementation that waits in sendNoWait can exit
		// before the test fails; this avoids leaking its timeout goroutine.
		<-session.sendCh
		<-closeDone
		<-unrelatedDone
		t.Fatal("saturated reset admission blocked unrelated stream processing")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("closeTimeout blocked on saturated send queue")
	}

	session.streamLock.Lock()
	pending := session.pendingReset[timedOut.id]
	session.streamLock.Unlock()
	if pending == nil {
		t.Fatal("timeout reset was not retained while send queue was saturated")
	}
	<-session.sendCh
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("pending timeout reset was not admitted after queue capacity returned")
	}
	queued := <-session.sendCh
	if queuedHdr := header(queued.Hdr); queuedHdr.MsgType() != typeWindowUpdate || queuedHdr.StreamID() != timedOut.id || queuedHdr.Flags()&flagRST == 0 {
		t.Fatalf("unexpected pending timeout reset: %v", queuedHdr)
	}

	if got := session.NumStreams(); got != 1 {
		t.Fatalf("stream registry after timeout: got %d, want 1", got)
	}
	session.streamLock.Lock()
	current := session.streams[unrelated.id]
	session.streamLock.Unlock()
	if current != unrelated {
		t.Fatalf("unrelated stream ownership changed: got %p, want %p", current, unrelated)
	}
	if got := atomic.LoadUint32(&unrelated.sendWindow); got != initialStreamWindow+1 {
		t.Fatalf("unrelated send window: got %d, want %d", got, initialStreamWindow+1)
	}
}

func TestSession_QueuedHeadersHaveRequestOwnership(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, time.Second)

	sharedHdr := header(make([]byte, headerSize))
	sharedHdr.encode(typePing, flagACK, 0, 1)
	if err := session.sendNoWait(sharedHdr); err != nil {
		t.Fatalf("queue first header: %v", err)
	}
	sharedHdr.encode(typePing, flagACK, 0, 2)
	if err := session.sendNoWait(sharedHdr); err != nil {
		t.Fatalf("queue second header: %v", err)
	}

	sendLoopErrCh := startSendLoop(session)
	first := awaitControlledWrite(t, conn)
	if got := header(first).Length(); got != 1 {
		t.Fatalf("first queued header was overwritten: got id %d", got)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(first)})
	second := awaitControlledWrite(t, conn)
	if got := header(second).Length(); got != 2 {
		t.Fatalf("second queued header changed: got id %d", got)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(second)})

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestSession_TransportWriteFailures(t *testing.T) {
	transportErr := errors.New("transport write failed")
	tests := []struct {
		name        string
		releaseBody bool
		result      controlledWriteResult
		wantErr     error
	}{
		{
			name:    "header error",
			result:  controlledWriteResult{err: transportErr},
			wantErr: transportErr,
		},
		{
			name:    "header short write",
			result:  controlledWriteResult{n: headerSize - 1},
			wantErr: io.ErrShortWrite,
		},
		{
			name:        "body error",
			releaseBody: true,
			result:      controlledWriteResult{err: transportErr},
			wantErr:     transportErr,
		},
		{
			name:        "body short write",
			releaseBody: true,
			result:      controlledWriteResult{n: len("payload") - 1},
			wantErr:     io.ErrShortWrite,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newControlledWriteConn()
			session := newSendTestSession(conn, time.Second)
			sendLoopErrCh := startSendLoop(session)
			stream := newStream(session, 1, streamEstablished)

			type writeResult struct {
				n   int
				err error
			}
			resultCh := make(chan writeResult, 1)
			go func() {
				n, err := stream.Write([]byte("payload"))
				resultCh <- writeResult{n: n, err: err}
			}()

			headerWrite := awaitControlledWrite(t, conn)
			if tc.releaseBody {
				releaseControlledWrite(t, conn, controlledWriteResult{n: len(headerWrite)})
				_ = awaitControlledWrite(t, conn)
				releaseControlledWrite(t, conn, controlledWriteResult{n: tc.result.n, err: tc.result.err})
			} else {
				releaseControlledWrite(t, conn, tc.result)
			}

			select {
			case result := <-resultCh:
				if result.n != 0 || !errors.Is(result.err, tc.wantErr) {
					t.Fatalf("write result: n=%d err=%v, want n=0 err=%v", result.n, result.err, tc.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for failed write result")
			}
			if window := atomic.LoadUint32(&stream.sendWindow); window != initialStreamWindow {
				t.Fatalf("send window changed after failed frame: got %d", window)
			}
			if err := awaitSendResult(t, sendLoopErrCh); !errors.Is(err, tc.wantErr) {
				t.Fatalf("send loop error: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestStreamReadData_FragmentedBody(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	hdr := header(make([]byte, headerSize))
	payload := []byte("fragmented payload")
	hdr.encode(typeData, 0, stream.id, uint32(len(payload)))

	if err := stream.readData(hdr, 0, &fragmentedReader{data: payload, step: 1}); err != nil {
		t.Fatalf("read fragmented DATA: %v", err)
	}
	stream.recvLock.Lock()
	got := append([]byte(nil), stream.recvBuf.Bytes()...)
	remaining := stream.recvWindow
	stream.recvLock.Unlock()
	if !bytes.Equal(got, payload) {
		t.Fatalf("fragmented DATA: got %q, want %q", got, payload)
	}
	if want := initialStreamWindow - uint32(len(payload)); remaining != want {
		t.Fatalf("receive window: got %d, want %d", remaining, want)
	}
}

func TestStreamReadData_ExactLengthPreservesReaderError(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	hdr := header(make([]byte, headerSize))
	payload := []byte("exact")
	hdr.encode(typeData, 0, stream.id, uint32(len(payload)))
	transportErr := errors.New("reader failed after complete frame")

	err := stream.readData(hdr, 0, &terminalErrorReader{data: payload, err: transportErr})
	if !errors.Is(err, transportErr) {
		t.Fatalf("exact DATA error: got %v, want %v", err, transportErr)
	}
	stream.recvLock.Lock()
	got := append([]byte(nil), stream.recvBuf.Bytes()...)
	remaining := stream.recvWindow
	stream.recvLock.Unlock()
	if !bytes.Equal(got, payload) {
		t.Fatalf("exact DATA bytes: got %q, want %q", got, payload)
	}
	if remaining != initialStreamWindow {
		t.Fatalf("receive window after reader error: got %d, want %d", remaining, initialStreamWindow)
	}
}

func TestStreamReadData_DoesNotConsumeNextFrame(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	hdr := header(make([]byte, headerSize))
	payload := []byte("frame body")
	nextFrame := []byte("next frame header")
	hdr.encode(typeData, 0, stream.id, uint32(len(payload)))
	reader := bufio.NewReader(bytes.NewReader(append(append([]byte(nil), payload...), nextFrame...)))

	if err := stream.readData(hdr, 0, reader); err != nil {
		t.Fatalf("read DATA: %v", err)
	}
	remaining, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read bytes after DATA: %v", err)
	}
	if !bytes.Equal(remaining, nextFrame) {
		t.Fatalf("bytes after DATA: got %q, want %q", remaining, nextFrame)
	}
}

func TestStreamReadData_TruncatedBodyRetainsPartialBytes(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	stream := newStream(session, 1, streamEstablished)
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeData, 0, stream.id, 5)

	err := stream.readData(hdr, 0, bytes.NewReader([]byte("cut")))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated DATA error: got %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if !stream.recvLock.TryLock() {
		t.Fatal("receive lock remained held after truncated DATA")
	}
	gotBytes := append([]byte(nil), stream.recvBuf.Bytes()...)
	gotWindow := stream.recvWindow
	stream.recvLock.Unlock()
	if !bytes.Equal(gotBytes, []byte("cut")) {
		t.Fatalf("partial DATA bytes: got %q, want %q", gotBytes, "cut")
	}
	if gotWindow != initialStreamWindow {
		t.Fatalf("receive window after truncated DATA: got %d, want %d", gotWindow, initialStreamWindow)
	}
}

func TestSession_MissingStreamTruncatedData(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	session.bufRead = bufio.NewReader(bytes.NewReader([]byte("cut")))
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeData, 0, 1, 5)

	err := session.handleStreamMessage(hdr)
	if err != io.EOF {
		t.Fatalf("missing-stream truncated DATA error: got %v, want %v", err, io.EOF)
	}
}

func TestSession_MissingStreamDrainErrorStopsReceive(t *testing.T) {
	session := newOpenTestSession(nil, time.Second)
	transportErr := errors.New("body transport failed")
	dataHdr := header(make([]byte, headerSize))
	dataHdr.encode(typeData, 0, 1, 8*1024)
	goAwayHdr := header(make([]byte, headerSize))
	goAwayHdr.encode(typeGoAway, 0, 0, goAwayNormal)
	reader := &scriptedReader{
		chunks: [][]byte{dataHdr, bytes.Repeat([]byte{'x'}, 8*1024), goAwayHdr},
		errs:   []error{nil, transportErr, nil},
	}
	session.bufRead = bufio.NewReaderSize(reader, 4096)

	err := session.recvLoop()
	if !errors.Is(err, transportErr) {
		t.Fatalf("recvLoop error: got %v, want %v", err, transportErr)
	}
	select {
	case <-session.recvDoneCh:
	default:
		t.Fatal("recvDoneCh was not closed on receive error")
	}
	if atomic.LoadInt32(&session.remoteGoAway) != 0 {
		t.Fatal("recvLoop parsed GoAway after the drain error")
	}
	if reader.reads != 2 {
		t.Fatalf("reader calls after drain error: got %d, want 2", reader.reads)
	}
}

func TestSessionCloseReleasesStreamOwnership(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := newStream(session, 1, streamInit)
	stream.stateLock.Lock()
	stream.closeTimer = time.AfterFunc(time.Hour, func() {})
	stream.stateLock.Unlock()

	session.streamLock.Lock()
	session.streams[stream.id] = stream
	session.inflight[stream.id] = struct{}{}
	session.synCh <- struct{}{}
	session.streamLock.Unlock()
	prepareSessionClose(t, session)
	close(session.sendDoneCh)

	if err := session.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
	stream.stateLock.Lock()
	state := stream.state
	terminal := stream.terminal
	timer := stream.closeTimer
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal {
		t.Fatalf("stream state after Session.Close: state=%d terminal=%v", state, terminal)
	}
	if timer != nil {
		t.Fatalf("stream close timer after Session.Close: %v", timer)
	}
	if got := session.NumStreams(); got != 0 {
		t.Fatalf("stream registry after Session.Close: got %d, want 0", got)
	}
	session.streamLock.Lock()
	inflight := len(session.inflight)
	session.streamLock.Unlock()
	if inflight != 0 {
		t.Fatalf("inflight registry after Session.Close: got %d, want 0", inflight)
	}
	assertSynCreditAvailable(t, session)
}

func TestStreamCloseTimeoutPreservesReplacementOwnership(t *testing.T) {
	session := newOpenTestSession(nil, time.Second)
	session.sendCh = make(chan *sendReady, 1)
	oldStream := newStream(session, 1, streamEstablished)
	replacement := newStream(session, 1, streamEstablished)
	session.streamLock.Lock()
	session.streams[replacement.id] = replacement
	session.streamLock.Unlock()

	oldStream.closeTimeout()

	session.streamLock.Lock()
	current := session.streams[replacement.id]
	session.streamLock.Unlock()
	if current != replacement {
		t.Fatalf("replacement stream ownership: got %p, want %p", current, replacement)
	}
	if queued := len(session.sendCh); queued != 0 {
		t.Fatalf("stale timeout queued RST for replacement: %d request(s)", queued)
	}
	oldStream.stateLock.Lock()
	state := oldStream.state
	terminal := oldStream.terminal
	oldStream.stateLock.Unlock()
	if state != streamClosed || !terminal {
		t.Fatalf("timed-out stream state: state=%d terminal=%v", state, terminal)
	}
}

func TestResetStreamOwnershipSerializesRSTEnqueue(t *testing.T) {
	session := newOpenTestSession(nil, time.Second)
	oldStream := newStream(session, 1, streamEstablished)
	session.streamLock.Lock()
	session.streams[oldStream.id] = oldStream
	session.streamLock.Unlock()

	enqueueStarted := make(chan struct{})
	releaseEnqueue := make(chan struct{})
	queuedHeader := make(chan header, 1)
	resetDone := make(chan bool, 1)
	go func() {
		resetDone <- session.resetStreamIfOwned(oldStream, func(hdr header) error {
			close(enqueueStarted)
			<-releaseEnqueue
			queuedHeader <- hdr
			return nil
		})
	}()
	<-enqueueStarted
	if session.streamLock.TryLock() {
		session.streamLock.Unlock()
		t.Fatal("registry lock released before RST enqueue")
	}

	replacementDone := make(chan error, 1)
	go func() { replacementDone <- session.incomingStream(oldStream.id) }()
	close(releaseEnqueue)
	queued := <-queuedHeader
	if queued.MsgType() != typeWindowUpdate || queued.StreamID() != oldStream.id || queued.Flags()&flagRST == 0 {
		t.Fatalf("unexpected queued reset: %v", queued)
	}
	if err := <-replacementDone; err != nil {
		t.Fatalf("register replacement: %v", err)
	}
	if !<-resetDone {
		t.Fatal("owned stream was not removed after RST enqueue")
	}

	session.streamLock.Lock()
	current := session.streams[oldStream.id]
	session.streamLock.Unlock()
	if current == nil || current == oldStream {
		t.Fatalf("replacement ownership after reset: got %p", current)
	}
}

func TestStreamCloseTimeoutSendsRSTWhenOwned(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := newStream(session, 1, streamEstablished)
	session.streamLock.Lock()
	session.streams[stream.id] = stream
	session.streamLock.Unlock()
	sendLoopErrCh := startSendLoop(session)

	stream.closeTimeout()
	frame := awaitControlledWrite(t, conn)
	hdr := header(frame)
	if hdr.MsgType() != typeWindowUpdate || hdr.StreamID() != stream.id || hdr.Flags()&flagRST == 0 {
		t.Fatalf("unexpected timeout RST: %v", hdr)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(frame)})
	stopSendLoop(t, session, sendLoopErrCh)
	if got := session.NumStreams(); got != 0 {
		t.Fatalf("stream registry after timeout RST: got %d, want 0", got)
	}
}

func TestSessionPingShutdownReleasesPendingOwnership(t *testing.T) {
	session := newSendTestSession(nil, time.Second)
	session.pings = make(map[uint32]chan struct{})
	session.sendCh = make(chan *sendReady)
	close(session.shutdownCh)

	if _, err := session.Ping(); err != ErrSessionShutdown {
		t.Fatalf("Ping after shutdown: got %v, want %v", err, ErrSessionShutdown)
	}
	session.pingLock.Lock()
	pending := len(session.pings)
	session.pingLock.Unlock()
	if pending != 0 {
		t.Fatalf("pending pings after shutdown: got %d, want 0", pending)
	}
}

func TestStreamReadBufferedDataIgnoresShutdownWriteError(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, time.Second)
	stream := newStream(session, 1, streamEstablished)
	payload := bytes.Repeat([]byte{'x'}, int(session.config.MaxStreamWindowSize))
	stream.recvLock.Lock()
	stream.recvBuf = bytes.NewBuffer(payload)
	stream.recvWindow = 0
	stream.recvLock.Unlock()
	sendLoopErrCh := startSendLoop(session)

	resultCh := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		buf := make([]byte, len(payload))
		n, err := stream.Read(buf)
		resultCh <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	frame := awaitControlledWrite(t, conn)
	if hdr := header(frame); hdr.MsgType() != typeWindowUpdate || hdr.StreamID() != stream.id {
		t.Fatalf("unexpected buffered-read window update: %v", hdr)
	}
	transportErr := errors.New("window update transport failed")
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(frame), err: transportErr})
	result := <-resultCh
	if result.n != len(payload) || result.err != nil {
		t.Fatalf("buffered read after session shutdown: n=%d err=%v", result.n, result.err)
	}
	if err := awaitSendResult(t, sendLoopErrCh); !errors.Is(err, transportErr) {
		t.Fatalf("send loop error: got %v, want %v", err, transportErr)
	}
}

func TestStreamReadBufferedDataPropagatesRequestTimeout(t *testing.T) {
	session := newSendTestSession(nil, 20*time.Millisecond)
	stream := newStream(session, 1, streamEstablished)
	payload := []byte("buffered request error")
	stream.recvLock.Lock()
	stream.recvBuf = bytes.NewBuffer(append([]byte(nil), payload...))
	stream.recvWindow = 0
	stream.recvLock.Unlock()
	if session.shutdownError() != nil {
		t.Fatal("test session unexpectedly has a shutdown error")
	}

	got := make([]byte, len(payload))
	n, err := stream.Read(got)
	if n != len(payload) || !errors.Is(err, ErrConnectionWriteTimeout) {
		t.Fatalf("buffered read request timeout: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("buffered read bytes: got %q, want %q", got, payload)
	}
	if session.shutdownError() != nil {
		t.Fatalf("request timeout recorded as session shutdown: %v", session.shutdownError())
	}
}
func TestAcceptStreamRejectsQueuedStreamAfterShutdown(t *testing.T) {
	session := newOpenTestSession(nil, time.Second)
	stream := addIncomingTestStream(t, session, 1)

	session.shutdownLock.Lock()
	resultCh := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		accepted, err := session.AcceptStream()
		resultCh <- struct {
			stream *Stream
			err    error
		}{stream: accepted, err: err}
	}()
	waitForAcceptSelection(t, session)
	session.shutdown = true
	session.recordShutdownErr(ErrSessionShutdown)
	close(session.shutdownCh)
	session.shutdownLock.Unlock()

	result := <-resultCh
	if result.stream != nil || result.err != ErrSessionShutdown {
		t.Fatalf("AcceptStream after shutdown: stream=%p err=%v", result.stream, result.err)
	}
	if got := session.NumStreams(); got != 0 {
		t.Fatalf("stream registry after rejected accept: got %d, want 0", got)
	}
	stream.stateLock.Lock()
	terminal := stream.terminal
	stream.stateLock.Unlock()
	if !terminal {
		t.Fatal("queued stream was not force-closed")
	}
}

func TestAcceptStreamCloseUnblocksCommittedTransport(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := addIncomingTestStream(t, session, 1)
	prepareSessionClose(t, session)
	sendLoopErrCh := startSendLoop(session)

	resultCh := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		accepted, err := session.AcceptStream()
		resultCh <- struct {
			stream *Stream
			err    error
		}{stream: accepted, err: err}
	}()

	frame := awaitControlledWrite(t, conn)
	if hdr := header(frame); hdr.MsgType() != typeWindowUpdate || hdr.StreamID() != stream.id || hdr.Flags()&flagACK == 0 {
		t.Fatalf("unexpected accept ACK: %v", hdr)
	}

	closeResultCh := make(chan error, 1)
	go func() { closeResultCh <- session.Close() }()
	select {
	case err := <-closeResultCh:
		if err != nil {
			t.Fatalf("Session.Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Session.Close blocked behind committed AcceptStream transport")
	}

	result := <-resultCh
	if result.stream != nil || result.err == nil {
		t.Fatalf("AcceptStream after transport shutdown: stream=%p err=%v", result.stream, result.err)
	}
	if err := awaitSendResult(t, sendLoopErrCh); err == nil {
		t.Fatal("send loop returned nil after connection close")
	}
}

func TestSessionCloseDrainsAcceptedStreams(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := addIncomingTestStream(t, session, 1)
	prepareSessionClose(t, session)
	close(session.sendDoneCh)

	if err := session.Close(); err != nil {
		t.Fatalf("Session.Close: %v", err)
	}
	if queued := len(session.acceptCh); queued != 0 {
		t.Fatalf("accepted stream channel after Session.Close: %d", queued)
	}
	stream.stateLock.Lock()
	state := stream.state
	terminal := stream.terminal
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal {
		t.Fatalf("accepted stream after Session.Close: state=%d terminal=%v", state, terminal)
	}
}

func TestSession_QueuedWindowUpdateRollback(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, 20*time.Millisecond)
	stream := newStream(session, 1, streamEstablished)
	baseline := initialStreamWindow / 2
	stream.recvLock.Lock()
	stream.recvWindow = baseline
	stream.recvLock.Unlock()

	errCh := make(chan error, 1)
	go func() { errCh <- stream.sendWindowUpdate() }()

	select {
	case <-session.sendCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued window update")
	}
	if err := awaitSendResult(t, errCh); err != ErrConnectionWriteTimeout {
		t.Fatalf("queued window update result: got %v, want %v", err, ErrConnectionWriteTimeout)
	}

	stream.recvLock.Lock()
	restored := stream.recvWindow
	stream.recvLock.Unlock()
	if restored != baseline {
		t.Fatalf("recv window was not rolled back: got %d, want %d", restored, baseline)
	}

	session.config.ConnectionWriteTimeout = time.Second
	sendLoopErrCh := startSendLoop(session)
	updateErrCh := make(chan error, 1)
	go func() { updateErrCh <- stream.sendWindowUpdate() }()

	frame := awaitControlledWrite(t, conn)
	delta := initialStreamWindow - baseline
	if got := header(frame).MsgType(); got != typeWindowUpdate {
		t.Fatalf("unexpected window update type: %d", got)
	}
	if got := header(frame).Length(); got != delta {
		t.Fatalf("window update delta: got %d, want %d", got, delta)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(frame)})
	if err := awaitSendResult(t, updateErrCh); err != nil {
		t.Fatalf("successful window update: %v", err)
	}

	stream.recvLock.Lock()
	updated := stream.recvWindow
	stream.recvLock.Unlock()
	if updated != initialStreamWindow {
		t.Fatalf("recv window after commit: got %d, want %d", updated, initialStreamWindow)
	}

	// Feed the committed update to a peer-side stream to prove that the
	// advertised delta restores send credit after the canceled request.
	peer := newStream(session, 1, streamEstablished)
	atomic.StoreUint32(&peer.sendWindow, 0)
	if err := peer.incrSendWindow(header(frame), header(frame).Flags()); err != nil {
		t.Fatalf("peer window update: %v", err)
	}
	if got := atomic.LoadUint32(&peer.sendWindow); got != delta {
		t.Fatalf("peer send window: got %d, want %d", got, delta)
	}

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestSession_ShutdownWindowUpdateRollback(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, time.Second)
	session.sendCh = make(chan *sendReady)
	stream := newStream(session, 1, streamEstablished)
	baseline := initialStreamWindow / 2
	stream.recvLock.Lock()
	stream.recvWindow = baseline
	stream.recvLock.Unlock()
	close(session.shutdownCh)

	errCh := make(chan error, 1)
	go func() { errCh <- stream.sendWindowUpdate() }()
	if err := awaitSendResult(t, errCh); err != ErrSessionShutdown {
		t.Fatalf("shutdown window update result: got %v, want %v", err, ErrSessionShutdown)
	}

	stream.recvLock.Lock()
	restored := stream.recvWindow
	stream.recvLock.Unlock()
	if restored != baseline {
		t.Fatalf("recv window after shutdown cancellation: got %d, want %d", restored, baseline)
	}
	select {
	case frame := <-conn.writes:
		t.Fatalf("shutdown-canceled update reached wire: %v", frame)
	default:
	}
}

func TestSession_CanceledSYNACKRollback(t *testing.T) {
	tests := []struct {
		name          string
		initial       streamState
		terminalFlags uint16
		want          streamState
	}{
		{name: "syn canceled", initial: streamInit, want: streamInit},
		{name: "ack canceled after fin", initial: streamSYNReceived, terminalFlags: flagFIN, want: streamRemoteClose},
		{name: "syn canceled after rst", initial: streamInit, terminalFlags: flagRST, want: streamReset},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newControlledWriteConn()
			session := newSendTestSession(conn, 20*time.Millisecond)
			stream := newStream(session, 1, tc.initial)
			errCh := make(chan error, 1)
			go func() { errCh <- stream.sendWindowUpdate() }()

			select {
			case <-session.sendCh:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for queued SYN/ACK")
			}

			if tc.terminalFlags != 0 {
				if err := stream.processFlags(tc.terminalFlags); err != nil {
					t.Fatalf("process terminal flags: %v", err)
				}
			}
			if err := awaitSendResult(t, errCh); err != ErrConnectionWriteTimeout {
				t.Fatalf("queued SYN/ACK result: got %v, want %v", err, ErrConnectionWriteTimeout)
			}

			stream.stateLock.Lock()
			got := stream.state
			stream.stateLock.Unlock()
			if got != tc.want {
				t.Fatalf("stream state after cancellation: got %d, want %d", got, tc.want)
			}
		})
	}
}

func waitForQueuedOpen(t *testing.T, session *Session) *Stream {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		session.streamLock.Lock()
		var stream *Stream
		if len(session.streams) == 1 && len(session.sendCh) == 1 {
			for _, candidate := range session.streams {
				stream = candidate
			}
		}
		session.streamLock.Unlock()
		if stream != nil {
			return stream
		}
		select {
		case <-timeout.C:
			session.streamLock.Lock()
			inflight := len(session.inflight)
			session.streamLock.Unlock()
			t.Fatalf("timed out waiting for queued open: streams=%d inflight=%d queued=%d", session.NumStreams(), inflight, len(session.sendCh))
		default:
			runtime.Gosched()
		}
	}
}

func assertSynCreditAvailable(t *testing.T, session *Session) {
	t.Helper()
	select {
	case session.synCh <- struct{}{}:
	default:
		t.Fatal("SYN credit was not returned")
	}
}

func assertOpenTerminated(t *testing.T, stream *Stream) {
	t.Helper()
	stream.stateLock.Lock()
	state := stream.state
	terminal := stream.terminal
	openInFlight := stream.openInFlight
	openPending := stream.openPending
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal || openInFlight || openPending {
		t.Fatalf("open stream termination: state=%d terminal=%v inFlight=%v pending=%v", state, terminal, openInFlight, openPending)
	}
}

func TestSession_OpenStreamQueuedSYNCancellation(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, 20*time.Millisecond)
	session.config.StreamOpenTimeout = time.Hour
	session.sendCh = make(chan *sendReady, 1)

	blockerHdr := header(make([]byte, headerSize))
	blockerHdr.encode(typePing, 0, 0, 1)
	session.sendCh <- newSendReady(blockerHdr)
	sendLoopErrCh := startSendLoop(session)
	if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
		t.Fatalf("unexpected blocker frame: %v", got)
	}

	resultCh := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		stream, err := session.OpenStream()
		resultCh <- struct {
			stream *Stream
			err    error
		}{stream: stream, err: err}
	}()
	stream := waitForQueuedOpen(t, session)
	result := awaitOpenResult(t, resultCh)
	if result.stream != nil || result.err != ErrConnectionWriteTimeout {
		t.Fatalf("queued SYN result: stream=%v err=%v", result.stream, result.err)
	}
	assertOpenTerminated(t, stream)
	if session.NumStreams() != 0 {
		t.Fatalf("stream map after queued SYN cancellation: got %d", session.NumStreams())
	}
	session.streamLock.Lock()
	inflight := len(session.inflight)
	session.streamLock.Unlock()
	if inflight != 0 {
		t.Fatalf("inflight ownership after queued SYN cancellation: got %d", inflight)
	}
	assertSynCreditAvailable(t, session)

	releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled SYN reached wire: %v", frame)
	default:
	}
	stopSendLoop(t, session, sendLoopErrCh)
}

func awaitOpenResult(t *testing.T, resultCh <-chan struct {
	stream *Stream
	err    error
}) struct {
	stream *Stream
	err    error
} {
	t.Helper()
	select {
	case result := <-resultCh:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OpenStream result")
		return struct {
			stream *Stream
			err    error
		}{}
	}
}

func TestSession_OpenStreamQueuedSYNShutdownCancellation(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	session.config.StreamOpenTimeout = time.Hour
	session.sendCh = make(chan *sendReady, 1)

	blockerHdr := header(make([]byte, headerSize))
	blockerHdr.encode(typePing, 0, 0, 1)
	session.sendCh <- newSendReady(blockerHdr)
	sendLoopErrCh := startSendLoop(session)
	if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
		t.Fatalf("unexpected blocker frame: %v", got)
	}

	resultCh := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		stream, err := session.OpenStream()
		resultCh <- struct {
			stream *Stream
			err    error
		}{stream: stream, err: err}
	}()
	stream := waitForQueuedOpen(t, session)
	prepareSessionClose(t, session)
	closeResultCh := make(chan error, 1)
	go func() { closeResultCh <- session.Close() }()
	result := awaitOpenResult(t, resultCh)
	if result.stream != nil || result.err != ErrSessionShutdown {
		t.Fatalf("shutdown SYN result: stream=%v err=%v", result.stream, result.err)
	}
	if err := awaitSendResult(t, closeResultCh); err != nil {
		t.Fatalf("session close result: %v", err)
	}
	assertOpenTerminated(t, stream)
	if session.NumStreams() != 0 {
		t.Fatalf("stream map after shutdown SYN cancellation: got %d", session.NumStreams())
	}
	session.streamLock.Lock()
	inflight := len(session.inflight)
	session.streamLock.Unlock()
	if inflight != 0 {
		t.Fatalf("inflight ownership after shutdown SYN cancellation: got %d", inflight)
	}
	assertSynCreditAvailable(t, session)
	if err := awaitSendResult(t, sendLoopErrCh); err == nil {
		t.Fatal("send loop did not report transport shutdown")
	}
}

func TestSession_AcceptStreamQueuedACKCancellation(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, 20*time.Millisecond)
	session.sendCh = make(chan *sendReady, 1)

	blockerHdr := header(make([]byte, headerSize))
	blockerHdr.encode(typePing, 0, 0, 1)
	session.sendCh <- newSendReady(blockerHdr)
	sendLoopErrCh := startSendLoop(session)
	if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
		t.Fatalf("unexpected blocker frame: %v", got)
	}

	stream := addIncomingTestStream(t, session, 2)
	resultCh := make(chan struct {
		stream *Stream
		err    error
	}, 1)
	go func() {
		accepted, err := session.AcceptStream()
		resultCh <- struct {
			stream *Stream
			err    error
		}{stream: accepted, err: err}
	}()

	waitForQueuedOpen(t, session)
	result := awaitOpenResult(t, resultCh)
	if result.stream != nil || result.err != ErrConnectionWriteTimeout {
		t.Fatalf("queued ACK result: stream=%v err=%v", result.stream, result.err)
	}
	assertOpenTerminated(t, stream)
	if session.NumStreams() != 0 {
		t.Fatalf("stream map after queued ACK cancellation: got %d", session.NumStreams())
	}
	assertSynCreditAvailable(t, session)

	releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled ACK reached wire: %v", frame)
	default:
	}
	stopSendLoop(t, session, sendLoopErrCh)
}

func TestStreamCloseQueuedFINCancellation(t *testing.T) {
	tests := []struct {
		name            string
		initial         streamState
		canceledState   streamState
		committedState  streamState
		cleanupOnCommit bool
	}{
		{
			name:           "established",
			initial:        streamEstablished,
			canceledState:  streamEstablished,
			committedState: streamLocalClose,
		},
		{
			name:            "remote fin",
			initial:         streamRemoteClose,
			canceledState:   streamRemoteClose,
			committedState:  streamClosed,
			cleanupOnCommit: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := newControlledWriteConn()
			session := newSendTestSession(conn, 20*time.Millisecond)
			session.config.StreamCloseTimeout = time.Hour
			session.sendCh = make(chan *sendReady, 1)
			stream := newStream(session, 1, tc.initial)
			session.streams = map[uint32]*Stream{stream.id: stream}

			blockerHdr := header(make([]byte, headerSize))
			blockerHdr.encode(typePing, 0, 0, 1)
			session.sendCh <- newSendReady(blockerHdr)
			sendLoopErrCh := startSendLoop(session)
			if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
				t.Fatalf("unexpected blocker frame: %v", header(got))
			}
			firstCloseErrCh := make(chan error, 1)
			go func() { firstCloseErrCh <- stream.Close() }()
			queuedState := streamLocalClose
			if tc.initial == streamRemoteClose {
				queuedState = streamRemoteClose
			}
			waitForQueuedClose(t, session, stream, queuedState)
			if err := awaitSendResult(t, firstCloseErrCh); err != ErrConnectionWriteTimeout {
				t.Fatalf("first close result: got %v, want %v", err, ErrConnectionWriteTimeout)
			}
			select {
			case frame := <-conn.writes:
				t.Fatalf("canceled FIN reached wire: %v", frame)
			default:
			}

			stream.stateLock.Lock()
			state := stream.state
			closeTimer := stream.closeTimer
			stream.stateLock.Unlock()
			if state != tc.canceledState {
				t.Fatalf("state after queued FIN cancellation: got %d, want %d", state, tc.canceledState)
			}
			if closeTimer != nil {
				t.Fatalf("close timer after queued FIN cancellation: got %v, want nil", closeTimer)
			}
			if session.NumStreams() != 1 {
				t.Fatalf("stream map changed after queued FIN cancellation: got %d, want 1", session.NumStreams())
			}
			session.config.StreamCloseTimeout = 0

			releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
			select {
			case frame := <-conn.writes:
				t.Fatalf("canceled FIN reached wire after blocker release: %v", frame)
			default:
			}

			secondCloseErrCh := make(chan error, 1)
			go func() { secondCloseErrCh <- stream.Close() }()

			fin := awaitControlledWrite(t, conn)
			finHdr := header(fin)
			if finHdr.MsgType() != typeWindowUpdate || finHdr.StreamID() != stream.id || finHdr.Flags()&flagFIN == 0 {
				t.Fatalf("unexpected FIN frame: %v", finHdr)
			}
			releaseControlledWrite(t, conn, controlledWriteResult{n: len(fin)})
			if err := awaitSendResult(t, secondCloseErrCh); err != nil {
				t.Fatalf("second close result: %v", err)
			}
			select {
			case frame := <-conn.writes:
				t.Fatalf("duplicate FIN reached wire: %v", frame)
			default:
			}

			stream.stateLock.Lock()
			state = stream.state
			stream.stateLock.Unlock()
			if state != tc.committedState {
				t.Fatalf("state after committed FIN: got %d, want %d", state, tc.committedState)
			}
			if tc.cleanupOnCommit {
				if session.NumStreams() != 0 {
					t.Fatalf("stream map after committed remote FIN: got %d, want 0", session.NumStreams())
				}
			} else if session.NumStreams() != 1 {
				t.Fatalf("stream map after committed local FIN: got %d, want 1", session.NumStreams())
			}

			stopSendLoop(t, session, sendLoopErrCh)
		})
	}
}

func waitForQueuedClose(t *testing.T, session *Session, stream *Stream, want streamState) {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		stream.stateLock.Lock()
		pending := stream.closePending
		state := stream.state
		stream.stateLock.Unlock()
		if pending && state == want && len(session.sendCh) == 1 {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("timed out waiting for queued close: pending=%v state=%d queued=%d want=%d", pending, state, len(session.sendCh), want)
		default:
			runtime.Gosched()
		}
	}
}

func TestStreamCloseQueuedFINPreservesRemoteFIN(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, 200*time.Millisecond)
	session.config.StreamCloseTimeout = time.Hour
	session.sendCh = make(chan *sendReady, 1)
	stream := newStream(session, 1, streamEstablished)
	session.streams = map[uint32]*Stream{stream.id: stream}

	blockerHdr := header(make([]byte, headerSize))
	blockerHdr.encode(typePing, 0, 0, 1)
	session.sendCh <- newSendReady(blockerHdr)
	sendLoopErrCh := startSendLoop(session)
	if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
		t.Fatalf("unexpected blocker frame: %v", header(got))
	}

	firstCloseErrCh := make(chan error, 1)
	go func() { firstCloseErrCh <- stream.Close() }()
	waitForQueuedClose(t, session, stream, streamLocalClose)

	if err := stream.processFlags(flagFIN); err != nil {
		t.Fatalf("process remote FIN: %v", err)
	}
	stream.stateLock.Lock()
	state := stream.state
	pending := stream.closePending
	stream.stateLock.Unlock()
	if state != streamRemoteClose || !pending {
		t.Fatalf("remote FIN during pending close: state=%d pending=%v", state, pending)
	}
	if session.NumStreams() != 1 {
		t.Fatalf("stream map changed before local FIN completion: got %d, want 1", session.NumStreams())
	}

	if err := awaitSendResult(t, firstCloseErrCh); err != ErrConnectionWriteTimeout {
		t.Fatalf("first close result: got %v, want %v", err, ErrConnectionWriteTimeout)
	}
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled FIN reached wire: %v", frame)
	default:
	}
	stream.stateLock.Lock()
	state = stream.state
	if state != streamRemoteClose || stream.closePending || stream.closeTimer != nil {
		t.Fatalf("state after queued FIN cancellation: state=%d pending=%v timer=%v", state, stream.closePending, stream.closeTimer)
	}
	stream.stateLock.Unlock()
	if session.NumStreams() != 1 {
		t.Fatalf("stream map changed after queued FIN cancellation: got %d, want 1", session.NumStreams())
	}

	releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled FIN reached wire after blocker release: %v", frame)
	default:
	}

	secondCloseErrCh := make(chan error, 1)
	go func() { secondCloseErrCh <- stream.Close() }()
	fin := awaitControlledWrite(t, conn)
	finHdr := header(fin)
	if finHdr.MsgType() != typeWindowUpdate || finHdr.StreamID() != stream.id || finHdr.Flags()&flagFIN == 0 {
		t.Fatalf("unexpected local FIN: %v", finHdr)
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(fin)})
	if err := awaitSendResult(t, secondCloseErrCh); err != nil {
		t.Fatalf("second close result: %v", err)
	}
	select {
	case frame := <-conn.writes:
		t.Fatalf("duplicate FIN reached wire: %v", frame)
	default:
	}
	stream.stateLock.Lock()
	state = stream.state
	terminal := stream.terminal
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal {
		t.Fatalf("state after completed local FIN: state=%d terminal=%v", state, terminal)
	}
	if session.NumStreams() != 0 {
		t.Fatalf("stream map after completed local FIN: got %d, want 0", session.NumStreams())
	}

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestStreamCloseQueuedFINDoesNotResurrectAfterForceClose(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, 200*time.Millisecond)
	session.sendCh = make(chan *sendReady, 1)
	stream := newStream(session, 1, streamRemoteClose)
	session.streams = map[uint32]*Stream{stream.id: stream}

	blockerHdr := header(make([]byte, headerSize))
	blockerHdr.encode(typePing, 0, 0, 1)
	session.sendCh <- newSendReady(blockerHdr)
	sendLoopErrCh := startSendLoop(session)
	if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
		t.Fatalf("unexpected blocker frame: %v", header(got))
	}
	firstCloseErrCh := make(chan error, 1)
	go func() { firstCloseErrCh <- stream.Close() }()
	waitForQueuedClose(t, session, stream, streamRemoteClose)

	terminalCh := make(chan struct{})
	go func() {
		stream.forceClose()
		close(terminalCh)
	}()
	select {
	case <-terminalCh:
	case <-time.After(time.Second):
		t.Fatal("timed out publishing terminal stream state")
	}

	stream.stateLock.Lock()
	state := stream.state
	terminal := stream.terminal
	pending := stream.closePending
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal || pending {
		t.Fatalf("forceClose state: state=%d terminal=%v pending=%v", state, terminal, pending)
	}
	if session.NumStreams() != 1 {
		t.Fatalf("stream map changed by forceClose: got %d, want 1", session.NumStreams())
	}

	if err := awaitSendResult(t, firstCloseErrCh); err != ErrConnectionWriteTimeout {
		t.Fatalf("first close result: got %v, want %v", err, ErrConnectionWriteTimeout)
	}
	stream.stateLock.Lock()
	state = stream.state
	terminal = stream.terminal
	pending = stream.closePending
	stream.stateLock.Unlock()
	if state != streamClosed || !terminal || pending {
		t.Fatalf("state after canceled FIN callback: state=%d terminal=%v pending=%v", state, terminal, pending)
	}

	releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled FIN reached wire after forceClose: %v", frame)
	default:
	}
	select {
	case frame := <-conn.writes:
		t.Fatalf("canceled FIN reached wire after forceClose: %v", frame)
	default:
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close after forceClose: %v", err)
	}
	select {
	case frame := <-conn.writes:
		t.Fatalf("terminal stream sent duplicate FIN: %v", frame)
	default:
	}

	stopSendLoop(t, session, sendLoopErrCh)
}

func TestStreamConcurrentCloseSharesResult(t *testing.T) {
	t.Run("successful FIN", func(t *testing.T) {
		conn := newControlledWriteConn()
		session := newSendTestSession(conn, time.Second)
		session.sendCh = make(chan *sendReady, 1)
		stream := newStream(session, 1, streamEstablished)
		session.streams = map[uint32]*Stream{stream.id: stream}

		blockerHdr := header(make([]byte, headerSize))
		blockerHdr.encode(typePing, 0, 0, 1)
		session.sendCh <- newSendReady(blockerHdr)
		sendLoopErrCh := startSendLoop(session)
		if got := awaitControlledWrite(t, conn); header(got).MsgType() != typePing {
			t.Fatalf("unexpected blocker frame: %v", got)
		}

		firstErrCh := make(chan error, 1)
		go func() { firstErrCh <- stream.Close() }()
		waitForQueuedClose(t, session, stream, streamLocalClose)
		stream.stateLock.Lock()
		result := stream.closeResult
		waiterNotify := make(chan struct{}, 1)
		result.waiterNotify = waiterNotify
		stream.stateLock.Unlock()
		secondErrCh := make(chan error, 1)
		go func() {
			secondErrCh <- stream.Close()
		}()
		select {
		case <-waiterNotify:
		case <-time.After(time.Second):
			t.Fatal("second Close did not wait on the first close result")
		}

		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		fin := awaitControlledWrite(t, conn)
		if header(fin).Flags()&flagFIN == 0 {
			t.Fatalf("expected FIN frame: %v", header(fin))
		}
		releaseControlledWrite(t, conn, controlledWriteResult{n: len(fin)})
		if err := awaitSendResult(t, firstErrCh); err != nil {
			t.Fatalf("first Close result: %v", err)
		}
		if err := awaitSendResult(t, secondErrCh); err != nil {
			t.Fatalf("second Close result: %v", err)
		}
		select {
		case frame := <-conn.writes:
			t.Fatalf("duplicate FIN reached wire: %v", frame)
		default:
		}
		stopSendLoop(t, session, sendLoopErrCh)
	})

	t.Run("queued timeout is shared and retryable", func(t *testing.T) {
		conn := newControlledWriteConn()
		session := newSendTestSession(conn, 30*time.Millisecond)
		session.sendCh = make(chan *sendReady, 1)
		stream := newStream(session, 1, streamEstablished)
		session.streams = map[uint32]*Stream{stream.id: stream}

		blockerHdr := header(make([]byte, headerSize))
		blockerHdr.encode(typePing, 0, 0, 1)
		session.sendCh <- newSendReady(blockerHdr)
		sendLoopErrCh := startSendLoop(session)
		_ = awaitControlledWrite(t, conn)
		firstErrCh := make(chan error, 1)
		go func() { firstErrCh <- stream.Close() }()
		waitForQueuedClose(t, session, stream, streamLocalClose)
		stream.stateLock.Lock()
		result := stream.closeResult
		waiterNotify := make(chan struct{}, 1)
		result.waiterNotify = waiterNotify
		stream.stateLock.Unlock()
		secondErrCh := make(chan error, 1)
		go func() { secondErrCh <- stream.Close() }()
		select {
		case <-waiterNotify:
		case <-time.After(time.Second):
			t.Fatal("second Close did not wait on the first close result")
		}
		if err := awaitSendResult(t, firstErrCh); err != ErrConnectionWriteTimeout {
			t.Fatalf("first Close result: %v", err)
		}
		if err := awaitSendResult(t, secondErrCh); err != ErrConnectionWriteTimeout {
			t.Fatalf("second Close result: %v", err)
		}
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		select {
		case frame := <-conn.writes:
			t.Fatalf("canceled FIN reached wire: %v", frame)
		default:
		}

		session.config.ConnectionWriteTimeout = time.Second
		session.config.StreamCloseTimeout = 0
		retryErrCh := make(chan error, 1)
		go func() { retryErrCh <- stream.Close() }()
		fin := awaitControlledWrite(t, conn)
		if header(fin).Flags()&flagFIN == 0 {
			t.Fatalf("expected retry FIN frame: %v", header(fin))
		}
		releaseControlledWrite(t, conn, controlledWriteResult{n: len(fin)})
		if err := awaitSendResult(t, retryErrCh); err != nil {
			t.Fatalf("retry Close result: %v", err)
		}
		select {
		case frame := <-conn.writes:
			t.Fatalf("duplicate retry FIN reached wire: %v", frame)
		default:
		}
		stopSendLoop(t, session, sendLoopErrCh)
	})

	t.Run("RST does not release waiters early", func(t *testing.T) {
		conn := newControlledWriteConn()
		session := newSendTestSession(conn, 40*time.Millisecond)
		session.sendCh = make(chan *sendReady, 1)
		stream := newStream(session, 1, streamEstablished)
		session.streams = map[uint32]*Stream{stream.id: stream}

		blockerHdr := header(make([]byte, headerSize))
		blockerHdr.encode(typePing, 0, 0, 1)
		session.sendCh <- newSendReady(blockerHdr)
		sendLoopErrCh := startSendLoop(session)
		_ = awaitControlledWrite(t, conn)
		firstErrCh := make(chan error, 1)
		go func() { firstErrCh <- stream.Close() }()
		waitForQueuedClose(t, session, stream, streamLocalClose)
		stream.stateLock.Lock()
		result := stream.closeResult
		waiterNotify := make(chan struct{}, 1)
		result.waiterNotify = waiterNotify
		stream.stateLock.Unlock()
		secondErrCh := make(chan error, 1)
		go func() { secondErrCh <- stream.Close() }()
		select {
		case <-waiterNotify:
		case <-time.After(time.Second):
			t.Fatal("second Close did not wait on the first close result")
		}
		if err := stream.processFlags(flagRST); err != nil {
			t.Fatalf("process RST: %v", err)
		}
		if err := awaitSendResult(t, firstErrCh); err != ErrConnectionWriteTimeout {
			t.Fatalf("first RST-interleaved Close result: %v", err)
		}
		if err := awaitSendResult(t, secondErrCh); err != ErrConnectionWriteTimeout {
			t.Fatalf("second RST-interleaved Close result: %v", err)
		}
		releaseControlledWrite(t, conn, controlledWriteResult{n: headerSize})
		select {
		case frame := <-conn.writes:
			t.Fatalf("RST-canceled FIN reached wire: %v", frame)
		default:
		}
		if session.NumStreams() != 0 {
			t.Fatalf("stream map after RST: got %d", session.NumStreams())
		}
		stopSendLoop(t, session, sendLoopErrCh)
	})

	t.Run("Session.Close cancels and shares result", func(t *testing.T) {
		conn := newControlledWriteConn()
		session := newOpenTestSession(conn, time.Second)
		session.sendCh = make(chan *sendReady, 1)
		stream := newStream(session, 1, streamEstablished)
		session.streams[stream.id] = stream
		prepareSessionClose(t, session)

		blockerHdr := header(make([]byte, headerSize))
		blockerHdr.encode(typePing, 0, 0, 1)
		session.sendCh <- newSendReady(blockerHdr)
		sendLoopErrCh := startSendLoop(session)
		_ = awaitControlledWrite(t, conn)
		firstErrCh := make(chan error, 1)
		go func() { firstErrCh <- stream.Close() }()
		waitForQueuedClose(t, session, stream, streamLocalClose)
		stream.stateLock.Lock()
		result := stream.closeResult
		waiterNotify := make(chan struct{}, 1)
		result.waiterNotify = waiterNotify
		stream.stateLock.Unlock()
		secondErrCh := make(chan error, 1)
		go func() { secondErrCh <- stream.Close() }()
		select {
		case <-waiterNotify:
		case <-time.After(time.Second):
			t.Fatal("second Close did not wait on the first close result")
		}
		closeErrCh := make(chan error, 1)
		go func() { closeErrCh <- session.Close() }()
		if err := awaitSendResult(t, firstErrCh); err != ErrSessionShutdown {
			t.Fatalf("first Session.Close-interleaved result: %v", err)
		}
		if err := awaitSendResult(t, secondErrCh); err != ErrSessionShutdown {
			t.Fatalf("second Session.Close-interleaved result: %v", err)
		}
		if err := awaitSendResult(t, closeErrCh); err != nil {
			t.Fatalf("Session.Close result: %v", err)
		}
		stream.stateLock.Lock()
		state := stream.state
		terminal := stream.terminal
		stream.stateLock.Unlock()
		if state != streamClosed || !terminal {
			t.Fatalf("stream state after Session.Close cancellation: state=%d terminal=%v", state, terminal)
		}
		if err := awaitSendResult(t, sendLoopErrCh); err == nil {
			t.Fatal("send loop did not stop after Session.Close")
		}
	})
}

func TestStreamUnexpectedFINReleasesStateLock(t *testing.T) {
	conn := newControlledWriteConn()
	session := newOpenTestSession(conn, time.Second)
	stream := newStream(session, 1, streamRemoteClose)
	session.streams[stream.id] = stream

	errCh := make(chan error, 1)
	go func() { errCh <- stream.processFlags(flagFIN) }()
	if err := awaitSendResult(t, errCh); err != ErrUnexpectedFlag {
		t.Fatalf("unexpected FIN result: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		stream.stateLock.Lock()
		close(locked)
		stream.stateLock.Unlock()
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("stateLock remained held after unexpected FIN")
	}

	forced := make(chan struct{})
	go func() {
		stream.forceClose()
		close(forced)
	}()
	select {
	case <-forced:
	case <-time.After(time.Second):
		t.Fatal("forceClose deadlocked after unexpected FIN")
	}

	prepareSessionClose(t, session)
	close(session.sendDoneCh)
	closed := make(chan struct{})
	go func() {
		_ = session.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Session.Close deadlocked after unexpected FIN")
	}
}

func TestCancelAccept(t *testing.T) {
	_, server := testClientServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)

	go func() {
		stream, err := server.AcceptStreamWithContext(ctx)
		if err != context.Canceled {
			errCh <- err
			return
		}

		if stream != nil {
			defer func() { _ = stream.Close() }()
		}
		errCh <- nil
	}()

	cancel()

	drainErrorsUntil(t, errCh, 1, 0, "")
}

// drainErrorsUntil receives `expect` errors from errCh within `timeout`. Fails
// on any non-nil errors.
func drainErrorsUntil(t testing.TB, errCh chan error, expect int, timeout time.Duration, msg string) {
	t.Helper()
	start := time.Now()
	var timerC <-chan time.Time
	if timeout > 0 {
		timerC = time.After(timeout)
	}

	for found := 0; found < expect; {
		select {
		case <-timerC:
			t.Fatalf(msg+" (timeout was %v)", timeout)
		case err := <-errCh:
			if err != nil {
				t.Fatalf("err: %v", err)
			} else {
				found++
			}
		}
	}
	t.Logf("drain took %v (timeout was %v)", time.Since(start), timeout)
}
