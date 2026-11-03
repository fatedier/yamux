// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Hide all deadline methods, even when the wrapped transport has them.
type withoutWriteDeadline struct{ io.ReadWriteCloser }

type deadlineTestConn struct {
	net.Conn
	writeLock  sync.Mutex
	mu         sync.Mutex
	deadlines  []time.Time
	reject     bool
	rejectZero bool
}

func (c *deadlineTestConn) Write(p []byte) (int, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	return c.Conn.Write(p)
}

// Model a WebSocket Close that must acquire the active writer's lock.
func (c *deadlineTestConn) Close() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	return c.Conn.Close()
}

func (c *deadlineTestConn) SetWriteDeadline(d time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, d)
	c.mu.Unlock()
	if c.reject || (c.rejectZero && d.IsZero()) {
		return errors.New("write deadline unsupported")
	}
	return c.Conn.SetWriteDeadline(d)
}

func (c *deadlineTestConn) recordedDeadlines() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.deadlines...)
}

func deadlineTestConfig(keepalive bool) *Config {
	c := DefaultConfig()
	c.EnableKeepAlive = keepalive
	c.KeepAliveInterval = 40 * time.Millisecond
	c.ConnectionWriteTimeout = 200 * time.Millisecond
	c.LogOutput = io.Discard
	return c
}

func awaitDeadlineEvent(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for deadline event")
	}
}

func TestSessionPingCommittedBudget(t *testing.T) {
	// A successful Close does not necessarily release a transport Write.
	conn := newControlledWriteConn()
	session, err := Client(withoutWriteDeadline{conn}, deadlineTestConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	result := make(chan error, 1)
	start := time.Now()
	go func() { _, err := session.Ping(); result <- err }()
	awaitControlledWrite(t, conn)
	select {
	case err := <-result:
		if err != ErrConnectionWriteTimeout {
			t.Fatalf("committed Ping: %v", err)
		}
		t.Logf("committed Ping returned after %v", time.Since(start))
	case <-time.After(3 * session.config.ConnectionWriteTimeout):
		t.Fatal("committed Ping exceeded its write budget")
	}
	session.pingLock.Lock()
	pending := len(session.pings)
	session.pingLock.Unlock()
	if pending != 0 {
		t.Fatalf("pending Pings after timeout: %d", pending)
	}
}

func TestSessionPingCommittedShutdown(t *testing.T) {
	conn := newControlledWriteConn()
	session := newSendTestSession(conn, time.Hour)
	session.pings = make(map[uint32]chan struct{})
	loop := startSendLoop(session)
	defer func() { _ = conn.Close() }()
	result := make(chan error, 1)
	go func() { _, err := session.Ping(); result <- err }()
	hdr := awaitControlledWrite(t, conn)
	close(session.shutdownCh)
	// Keep the real transport Write blocked beyond the shutdown notification.
	select {
	case err := <-result:
		if err != ErrSessionShutdown {
			t.Fatalf("committed Ping during shutdown: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		// Release the writer before failing, so the negative control cannot leak.
		releaseControlledWrite(t, conn, controlledWriteResult{n: len(hdr)})
		_ = awaitSendResult(t, loop)
		t.Fatal("committed Ping ignored session shutdown")
	}
	releaseControlledWrite(t, conn, controlledWriteResult{n: len(hdr)})
	if err := awaitSendResult(t, loop); err != nil {
		t.Fatal(err)
	}
}

func TestSessionKeepaliveBlockedTransport(t *testing.T) {
	for _, mode := range []string{"missing", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			local, peer := net.Pipe()
			conn := &deadlineTestConn{Conn: local, reject: true}
			var transport io.ReadWriteCloser = conn
			if mode == "missing" {
				transport = withoutWriteDeadline{conn}
			}
			session, err := Client(transport, deadlineTestConfig(true))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = session.Close() }()
			defer func() { _ = peer.Close() }() // Release the old transport even in the negative control.
			start := time.Now()
			select {
			case <-session.CloseChan():
				t.Logf("keepalive closed %s-deadline session after %v", mode, time.Since(start))
			case <-time.After(4 * session.config.ConnectionWriteTimeout):
				t.Fatal("keepalive did not close session with blocked transport")
			}
			if err := session.shutdownError(); err != ErrKeepAliveTimeout {
				t.Fatalf("keepalive termination: %v", err)
			}
		})
	}
}

func TestSessionSendWriteDeadlineReleasesClose(t *testing.T) {
	local, peer := net.Pipe()
	conn := &deadlineTestConn{Conn: local}
	session, err := Client(conn, deadlineTestConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	defer func() { _ = peer.Close() }()
	start := time.Now()
	// GoAway retains the committed-send contract, unlike Ping.
	result := make(chan error, 1)
	go func() { result <- session.GoAway() }()
	select {
	case err := <-result:
		if err != ErrConnectionWriteTimeout {
			t.Fatalf("blocked transport write: %v", err)
		}
	case <-time.After(4 * session.config.ConnectionWriteTimeout):
		_ = peer.Close()
		_ = awaitSendResult(t, result)
		t.Fatal("transport write exceeded its deadline")
	}
	closed := make(chan struct{})
	go func() { _ = session.Close(); close(closed) }()
	awaitDeadlineEvent(t, closed)
	if err := session.shutdownError(); err != ErrConnectionWriteTimeout {
		t.Fatalf("session termination: %v", err)
	}
	deadlines := conn.recordedDeadlines()
	if len(deadlines) != 2 || deadlines[0].IsZero() || !deadlines[1].IsZero() {
		t.Fatalf("arm/clear deadlines: %v", deadlines)
	}
	t.Logf("Write and lock-taking Close completed after %v", time.Since(start))
}

type scriptedDeadlineConn struct {
	write     func([]byte) (int, error)
	deadlines []time.Time // Only accessed by sendLoop, inspected after it exits.
	clearErr  error
	setErr    error
	armErrAt  int
	armCalls  int
}

func (c *scriptedDeadlineConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (c *scriptedDeadlineConn) Write(p []byte) (int, error) { return c.write(p) }
func (c *scriptedDeadlineConn) Close() error                { return nil }
func (c *scriptedDeadlineConn) SetWriteDeadline(d time.Time) error {
	c.deadlines = append(c.deadlines, d)
	if d.IsZero() {
		return c.clearErr
	}
	c.armCalls++
	if c.armErrAt == 0 || c.armCalls == c.armErrAt {
		return c.setErr
	}
	return nil
}

func TestSessionFrameDeadlineAndErrors(t *testing.T) {
	transportErr := errors.New("transport failure")
	for _, tc := range []struct {
		name      string
		failWrite int
		err       error
		want      error
	}{
		{"success", 0, nil, nil},
		{"partial-header-timeout", 1, os.ErrDeadlineExceeded, ErrConnectionWriteTimeout},
		{"partial-body-timeout", 2, os.ErrDeadlineExceeded, ErrConnectionWriteTimeout},
		{"short-header", 1, nil, io.ErrShortWrite},
		{"body-transport-error", 2, transportErr, transportErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writes := 0
			conn := &scriptedDeadlineConn{}
			conn.write = func(p []byte) (int, error) {
				writes++
				if len(conn.deadlines) != 1 || conn.deadlines[0].IsZero() {
					t.Errorf("header/body must share one armed deadline: %v", conn.deadlines)
				}
				if writes == tc.failWrite {
					return len(p) / 2, tc.err
				}
				return len(p), nil
			}
			session := newSendTestSession(conn, 200*time.Millisecond)
			var canceled atomic.Bool
			request := newSendReadyWithHooks(make(header, headerSize), []byte("payload"), func() { canceled.Store(true) })
			session.sendCh <- request
			loop := startSendLoop(session)
			if err := awaitSendResult(t, request.done); err != tc.want {
				t.Fatalf("transport completion: got %v want %v", err, tc.want)
			}
			if tc.want == nil {
				stopSendLoop(t, session, loop)
			} else if err := awaitSendResult(t, loop); err != tc.want {
				t.Fatalf("send loop: got %v want %v", err, tc.want)
			}
			if canceled.Load() || request.cancel() {
				t.Fatal("committed partial frame was reported as queued cancellation")
			}
			if len(conn.deadlines) != 2 || !conn.deadlines[1].IsZero() {
				t.Fatalf("deadline not cleared on frame completion: %v", conn.deadlines)
			}
		})
	}
}

func TestSessionDeadlineSetterFallback(t *testing.T) {
	for _, mode := range []string{"arm-error", "clear-error", "success"} {
		t.Run(mode, func(t *testing.T) {
			conn := &scriptedDeadlineConn{write: func(p []byte) (int, error) { return len(p), nil }}
			if mode == "arm-error" {
				conn.setErr = errors.New("unsupported")
			}
			if mode == "clear-error" {
				conn.clearErr = errors.New("cannot clear")
			}
			session := newSendTestSession(conn, time.Second)
			loop := startSendLoop(session)
			for range 2 {
				if err := session.GoAway(); err != nil {
					t.Fatalf("setter failure must not fail transport write: %v", err)
				}
			}
			stopSendLoop(t, session, loop)
			want := map[string]int{"arm-error": 1, "clear-error": 4, "success": 4}[mode]
			if len(conn.deadlines) != want {
				t.Fatalf("deadline calls: got %v want %d", conn.deadlines, want)
			}
		})
	}
}

func TestSessionDeadlineClearFailureKeepsSessionHealthy(t *testing.T) {
	local, peer := net.Pipe()
	conn := &deadlineTestConn{Conn: local, rejectZero: true}
	config := deadlineTestConfig(false)
	session, err := Client(conn, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	defer func() { _ = peer.Close() }()
	// The peer continuously drains frames and responds, so a write timeout
	// cannot be attributed to an unresponsive application or a missing ACK.
	server, err := Server(peer, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	start := time.Now()
	ticker := time.NewTicker(config.ConnectionWriteTimeout / 4)
	defer ticker.Stop()
	for i := range 21 {
		if i > 0 {
			<-ticker.C
		}
		if _, err := session.Ping(); err != nil {
			t.Fatalf("healthy Ping %d after %v: %v", i, time.Since(start), err)
		}
	}
	if session.IsClosed() || server.IsClosed() {
		t.Fatal("healthy session closed after a rejected deadline clear")
	}
	if elapsed := time.Since(start); elapsed < 5*config.ConnectionWriteTimeout {
		t.Fatalf("probe did not cross five write budgets: %v", elapsed)
	}
	t.Logf("21 Pings over %v, write budget %v; rejected clears did not close the session", time.Since(start), config.ConnectionWriteTimeout)
}

func TestSessionDeadlineRearmFailure(t *testing.T) {
	for _, uncleared := range []bool{false, true} {
		t.Run(map[bool]string{false: "previous-clear-succeeded", true: "previous-clear-failed"}[uncleared], func(t *testing.T) {
			refreshErr := errors.New("cannot refresh deadline")
			writes := 0
			conn := &scriptedDeadlineConn{
				write:    func(p []byte) (int, error) { writes++; return len(p), nil },
				setErr:   refreshErr,
				armErrAt: 2,
			}
			if uncleared {
				conn.clearErr = errors.New("cannot clear deadline")
			}
			session := newOpenTestSession(conn, time.Second)
			prepareSessionClose(t, session)
			go session.send()
			defer func() { _ = session.Close() }()
			if err := session.GoAway(); err != nil {
				t.Fatalf("first frame: %v", err)
			}
			err := session.GoAway()
			if uncleared {
				if err != refreshErr {
					t.Fatalf("unmanageable armed deadline: got %v, want %v", err, refreshErr)
				}
				awaitDeadlineEvent(t, session.CloseChan())
				if err := session.shutdownError(); err != refreshErr {
					t.Fatalf("session termination: %v", err)
				}
			} else if err != nil {
				t.Fatalf("no deadline remains, so rearm failure must fall back: %v", err)
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			want := 2
			if uncleared {
				want = 1
			}
			if writes != want {
				t.Fatalf("transport writes: got %d, want %d", writes, want)
			}
		})
	}
}

func TestErrTimeoutTemporary(t *testing.T) {
	type temporaryError interface {
		Temporary() bool
	}
	err := error(ErrTimeout)
	temporaryErr, ok := err.(temporaryError)
	if !ErrTimeout.Timeout() || !os.IsTimeout(err) || !ok || !temporaryErr.Temporary() {
		t.Fatalf("ErrTimeout lost its net.Error timeout/temporary contract: %v", err)
	}
}

type closeDeadlineConn struct {
	*controlledWriteConn
	mu            sync.Mutex
	deadline      time.Time
	closeDeadline time.Time
	closeStarted  chan struct{}
	closeRelease  chan struct{}
	closeOnce     sync.Once
	clearStarted  chan struct{}
	clearRelease  chan struct{}
}

func (c *closeDeadlineConn) SetWriteDeadline(d time.Time) error {
	if d.IsZero() && c.clearStarted != nil {
		close(c.clearStarted)
		<-c.clearRelease
	}
	c.mu.Lock()
	c.deadline = d
	c.mu.Unlock()
	return nil
}

func (c *closeDeadlineConn) Close() error {
	c.closeOnce.Do(func() {
		// Model TLS closeNotify taking ownership of the transport deadline.
		_ = c.SetWriteDeadline(c.closeDeadline)
		close(c.closeStarted)
		<-c.closeRelease
		_ = c.controlledWriteConn.Close()
	})
	return nil
}

func TestSessionCloseOwnsWriteDeadline(t *testing.T) {
	for _, clearFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown-before-clear", true: "clear-before-shutdown"}[clearFirst], func(t *testing.T) {
			conn := &closeDeadlineConn{
				controlledWriteConn: newControlledWriteConn(),
				closeDeadline:       time.Now().Add(5 * time.Second),
				closeStarted:        make(chan struct{}),
				closeRelease:        make(chan struct{}),
			}
			if clearFirst {
				conn.clearStarted = make(chan struct{})
				conn.clearRelease = make(chan struct{})
			}
			session, err := Client(conn, deadlineTestConfig(false))
			if err != nil {
				t.Fatal(err)
			}
			closed := make(chan struct{})
			t.Cleanup(func() {
				close(conn.closeRelease)
				_ = conn.controlledWriteConn.Close()
				_ = session.Close()
			})
			result := make(chan error, 1)
			go func() { result <- session.GoAway() }()
			hdr := awaitControlledWrite(t, conn.controlledWriteConn)
			if clearFirst {
				releaseControlledWrite(t, conn.controlledWriteConn, controlledWriteResult{n: len(hdr)})
				awaitDeadlineEvent(t, conn.clearStarted)
			}
			go func() { _ = session.Close(); close(closed) }()
			awaitDeadlineEvent(t, session.CloseChan())
			if clearFirst {
				// Close must wait for an in-flight setter before taking ownership.
				overlapped := false
				select {
				case <-conn.closeStarted:
					overlapped = true
				case <-time.After(20 * time.Millisecond):
				}
				close(conn.clearRelease)
				if overlapped {
					t.Error("transport Close overlapped the previous deadline setter")
				}
			} else {
				awaitDeadlineEvent(t, conn.closeStarted)
				releaseControlledWrite(t, conn.controlledWriteConn, controlledWriteResult{n: len(hdr)})
			}
			if err := awaitSendResult(t, result); err != nil {
				t.Fatalf("committed write completion: %v", err)
			}
			awaitDeadlineEvent(t, conn.closeStarted)
			conn.mu.Lock()
			deadline := conn.deadline
			conn.mu.Unlock()
			if deadline != conn.closeDeadline {
				t.Fatalf("sendLoop overwrote Close's deadline: got %v want %v", deadline, conn.closeDeadline)
			}
		})
	}
}
