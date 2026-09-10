// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: MPL-2.0

package yamux

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type streamState int

const (
	streamInit streamState = iota
	streamSYNSent
	streamSYNReceived
	streamEstablished
	streamLocalClose
	streamRemoteClose
	streamClosed
	streamReset
)

type streamCloseResult struct {
	done         chan struct{}
	err          error
	waiterNotify chan struct{}
}

// Stream is used to represent a logical stream within a session. Methods on
// Stream are safe to call concurrently with one another, but all Read calls
// must be on the same goroutine and all Write calls must be on the same
// goroutine.
type Stream struct {
	recvWindow uint32
	sendWindow uint32

	id      uint32
	session *Session

	state     streamState
	stateLock sync.Mutex

	// closePending prevents concurrent Close calls from sending duplicate FINs.
	// A committed transport failure leaves it set until session teardown so a
	// failed FIN is never retried on the unusable connection.
	// terminal prevents a queued FIN cancellation from resurrecting a stream
	// after forceClose or RST has published a terminal state.
	closePending bool
	closeResult  *streamCloseResult
	terminal     bool
	// resetResult retains the single local RST attempt, including its error.
	// resetCh is closed only by a local Reset, to cancel queued sends.
	resetResult *streamCloseResult
	resetCh     chan struct{}
	// openInFlight tracks ownership of a stream that is not yet established.
	// openPending is true only while its SYN/ACK request is still queued.
	openInFlight bool
	openPending  bool

	recvBuf  *bytes.Buffer
	recvLock sync.Mutex
	// recvReset is protected by recvLock so an in-flight frame cannot refill
	// the receive buffer after either a local or remote reset.
	recvReset bool
	// recvInFlight reserves the unpublished tail of recvBuf for recvLoop.
	// A normal close must wait for that frame before reporting EOF. Reset
	// discards it immediately. Protected by recvLock.
	recvInFlight bool

	controlHdr     header
	controlHdrLock sync.Mutex

	sendHdr  header
	sendLock sync.Mutex

	recvNotifyCh chan struct{}
	sendNotifyCh chan struct{}

	readDeadline  atomic.Value // time.Time
	writeDeadline atomic.Value // time.Time

	// establishCh is notified if the stream is established or being closed.
	establishCh chan struct{}

	// closeTimer is set with stateLock held to honor the StreamCloseTimeout
	// setting on Session.
	closeTimer *time.Timer
}

// newStream is used to construct a new stream within
// a given session for an ID
func newStream(session *Session, id uint32, state streamState) *Stream {
	s := &Stream{
		id:           id,
		session:      session,
		state:        state,
		controlHdr:   header(make([]byte, headerSize)),
		sendHdr:      header(make([]byte, headerSize)),
		recvWindow:   initialStreamWindow,
		sendWindow:   initialStreamWindow,
		recvNotifyCh: make(chan struct{}, 1),
		sendNotifyCh: make(chan struct{}, 1),
		establishCh:  make(chan struct{}, 1),
		resetCh:      make(chan struct{}),
	}
	if state == streamInit || state == streamSYNReceived {
		s.openInFlight = true
	}
	s.readDeadline.Store(time.Time{})
	s.writeDeadline.Store(time.Time{})
	return s
}

// Session returns the associated stream session
func (s *Stream) Session() *Session {
	return s.session
}

// StreamID returns the ID of this stream
func (s *Stream) StreamID() uint32 {
	return s.id
}

// Read is used to read from the stream. It is safe to call Write, Read, and/or
// Close or Reset concurrently with each other, but calls to Read are not reentrant and
// should not be called from multiple goroutines. Multiple Read goroutines would
// receive different chunks of data from the Stream and be unable to reassemble
// them in order or along message boundaries, and may encounter deadlocks.
func (s *Stream) Read(b []byte) (n int, err error) {
	defer asyncNotify(s.recvNotifyCh)
START:

	// If the stream is closed and there's no data buffered, return EOF
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose:
		// LocalClose only prohibits further local writes. Handle reads normally.
	case streamRemoteClose:
		fallthrough
	case streamClosed:
		s.recvLock.Lock()
		if !s.recvInFlight && (s.recvBuf == nil || s.recvBuf.Len() == 0) {
			s.recvLock.Unlock()
			s.stateLock.Unlock()
			return 0, io.EOF
		}
		s.recvLock.Unlock()
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	// If there is no data available, block
	s.recvLock.Lock()
	if s.recvBuf == nil || s.recvBuf.Len() == 0 {
		s.recvLock.Unlock()
		s.stateLock.Unlock()
		goto WAIT
	}

	// Read any bytes
	n, _ = s.recvBuf.Read(b)
	s.recvLock.Unlock()
	s.stateLock.Unlock()

	// Send a window update potentially
	err = s.sendWindowUpdate()
	if err == ErrSessionShutdown || (err != nil && err != ErrConnectionReset && s.session.isShuttingDown()) {
		// A failed window update may terminate the session after bytes have
		// already been read. Preserve those bytes, but never hide a reset.
		err = nil
	}
	return n, err

WAIT:
	var timeout <-chan time.Time
	var timer *time.Timer
	readDeadline := s.readDeadline.Load().(time.Time)
	if !readDeadline.IsZero() {
		delay := time.Until(readDeadline)
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	select {
	case <-s.session.shutdownCh:
	case <-s.recvNotifyCh:
	case <-timeout:
		return 0, ErrTimeout
	}
	if timer != nil {
		if !timer.Stop() {
			<-timeout
		}
	}
	goto START
}

// Write is used to write to the stream. It is safe to call Write, Read, and/or
// Close or Reset concurrently with each other, but calls to Write are not reentrant and
// should not be called from multiple goroutines.
func (s *Stream) Write(b []byte) (n int, err error) {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	// Even an empty write reports a reset, just like an empty Read.
	s.stateLock.Lock()
	reset := s.state == streamReset
	s.stateLock.Unlock()
	if reset {
		return 0, ErrConnectionReset
	}
	total := 0
	for total < len(b) {
		n, err := s.write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// write is used to write to the stream, may return on a short write. The
// stream send window is decremented only after the complete request succeeds.
// A queued timeout/shutdown therefore returns n=0, while a committed
// transport error also returns n=0 with the actual transport error.
func (s *Stream) write(b []byte) (n int, err error) {
	var flags uint16
	var flagsRollback func()
	var max uint32
	var body []byte
START:
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose:
		fallthrough
	case streamClosed:
		s.stateLock.Unlock()
		return 0, ErrStreamClosed
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	window := atomic.LoadUint32(&s.sendWindow)
	if window == 0 {
		goto WAIT
	}

	// Determine the flags if any. SYN/ACK transitions are provisional until
	// the request commits; a queued cancellation rolls them back.
	flags, flagsRollback = s.sendFlags()

	// Send up to our send window
	max = min(window, uint32(len(b)))
	body = b[:max]

	// Send the header
	s.sendHdr.encode(typeData, flags, s.id, max)
	if err = s.session.waitForSendErrWithAbort(s.sendHdr, body, flagsRollback, nil, s.resetCh); err != nil {
		return 0, err
	}

	// Reduce our send window
	atomic.AddUint32(&s.sendWindow, ^uint32(max-1))

	// Unlock
	return int(max), err

WAIT:
	var timeout <-chan time.Time
	var timer *time.Timer
	writeDeadline := s.writeDeadline.Load().(time.Time)
	if !writeDeadline.IsZero() {
		delay := time.Until(writeDeadline)
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	select {
	case <-s.session.shutdownCh:
	case <-s.sendNotifyCh:
	case <-timeout:
		return 0, ErrTimeout
	}
	if timer != nil {
		if !timer.Stop() {
			<-timeout
		}
	}
	goto START
}

// sendFlags determines any flags that are appropriate based on the current
// stream state. The state transition is provisional until the containing send
// request commits. The returned rollback is only applied if the request is
// canceled while queued; terminal transitions made by the receive path win.
func (s *Stream) sendFlags() (uint16, func()) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	var flags uint16
	var from, to streamState
	switch s.state {
	case streamInit:
		flags |= flagSYN
		from, to = s.state, streamSYNSent
		s.state = streamSYNSent
		s.openPending = true
	case streamSYNReceived:
		flags |= flagACK
		from, to = s.state, streamEstablished
		s.state = streamEstablished
		s.openPending = true
	}
	if flags == 0 {
		return 0, nil
	}
	rollback := func() {
		s.stateLock.Lock()
		if s.state == to && s.openPending {
			s.state = from
			s.openPending = false
		}
		s.stateLock.Unlock()
	}
	return flags, rollback
}

// sendWindowUpdate potentially sends a window update enabling
// further writes to take place. Must be invoked with the lock.
func (s *Stream) sendWindowUpdate() error {
	return s.sendWindowUpdateWithHooks(nil)
}

// sendWindowUpdateWithHooks potentially sends a window update and invokes
// onCancel only after a queued request has been canceled and its provisional
// effects have been rolled back.
func (s *Stream) sendWindowUpdateWithHooks(onCancel func()) error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	// Determine the delta update
	max := s.session.config.MaxStreamWindowSize
	// Determine the flags before taking recvLock so the lock order remains
	// stateLock -> recvLock, matching the read-side terminal check.
	flags, flagsRollback := s.sendFlags()

	var bufLen uint32
	s.recvLock.Lock()
	if s.recvBuf != nil {
		bufLen = uint32(s.recvBuf.Len())
	}
	delta := (max - bufLen) - s.recvWindow

	// Check if we can omit the update
	if delta < (max/2) && flags == 0 {
		s.recvLock.Unlock()
		return nil
	}

	// Reserve the advertised credit before enqueueing so a committed header
	// cannot race a peer that starts using the update immediately. A queued
	// cancellation rolls this reservation back; committed requests never do.
	s.recvWindow += delta
	s.recvLock.Unlock()

	// Send the header
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, delta)
	rollback := func() {
		s.recvLock.Lock()
		if s.recvWindow >= delta {
			s.recvWindow -= delta
		} else {
			// A peer must not consume more than the pre-reservation credit, but
			// clamp defensively instead of allowing uint32 underflow.
			s.recvWindow = 0
		}
		s.recvLock.Unlock()
		if flagsRollback != nil {
			flagsRollback()
		}
		if onCancel != nil {
			onCancel()
		}
	}
	var onCommit func()
	if flags&(flagSYN|flagACK) != 0 {
		onCommit = func() {
			s.stateLock.Lock()
			s.openPending = false
			s.stateLock.Unlock()
		}
	}
	if err := s.session.waitForSendErrWithAbort(s.controlHdr, nil, rollback, onCommit, s.resetCh); err != nil {
		return err
	}
	if flags&flagACK != 0 {
		s.stateLock.Lock()
		s.openInFlight = false
		s.stateLock.Unlock()
	}
	return nil
}

// sendClose is used to send a FIN. onCancel is invoked only when the queued
// request is canceled before sendLoop claims it.
func (s *Stream) sendClose(onCancel func()) error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	flags, flagsRollback := s.sendFlags()
	flags |= flagFIN
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, 0)
	rollback := func() {
		if flagsRollback != nil {
			flagsRollback()
		}
		if onCancel != nil {
			onCancel()
		}
	}
	var onCommit func()
	if flags&(flagSYN|flagACK) != 0 {
		onCommit = func() {
			s.stateLock.Lock()
			s.openPending = false
			s.stateLock.Unlock()
		}
	}
	if err := s.session.waitForSendErrWithAbort(s.controlHdr, nil, rollback, onCommit, s.resetCh); err != nil {
		return err
	}
	if flags&flagACK != 0 {
		s.stateLock.Lock()
		s.openInFlight = false
		s.stateLock.Unlock()
	}
	return nil
}

// Close half-closes the stream, prohibiting further writes while allowing
// reads until the peer closes. It is safe to call Close concurrently.
func (s *Stream) Close() error {
	remoteClosePath := false
	var previousState streamState
	var provisionalState streamState
	s.stateLock.Lock()
	if result := s.closeResult; result != nil {
		if result.waiterNotify != nil {
			select {
			case result.waiterNotify <- struct{}{}:
			default:
			}
		}
		s.stateLock.Unlock()
		<-result.done
		return result.err
	}
	switch s.state {
	// Opened means we need to signal a close
	case streamSYNSent:
		fallthrough
	case streamSYNReceived:
		fallthrough
	case streamEstablished:
		if s.closePending {
			s.stateLock.Unlock()
			return nil
		}
		previousState = s.state
		s.state = streamLocalClose
		provisionalState = streamLocalClose
		s.closePending = true
		goto SEND_CLOSE

	case streamLocalClose:
	case streamRemoteClose:
		if s.closePending {
			s.stateLock.Unlock()
			return nil
		}
		previousState = streamRemoteClose
		provisionalState = streamRemoteClose
		s.closePending = true
		remoteClosePath = true
		goto SEND_CLOSE

	case streamClosed:
	case streamReset:
	default:
		panic("unhandled state")
	}
	s.stateLock.Unlock()
	return nil
SEND_CLOSE:
	// This shouldn't happen (the more realistic scenario to cancel the
	// timer is via processFlags) but just in case this ever happens, we
	// cancel the timer to prevent dangling timers.
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}

	// If we have a StreamCloseTimeout set we start the timeout timer.
	// We do this only if we're not already closing the stream since that
	// means this was a graceful close.
	//
	// This prevents memory leaks if one side (this side) closes and the
	// remote side poorly behaves and never responds with a FIN to complete
	// the close. After the specified timeout, we clean our resources up no
	// matter what.
	if !remoteClosePath && s.session.config.StreamCloseTimeout > 0 {
		s.closeTimer = time.AfterFunc(
			s.session.config.StreamCloseTimeout, s.closeTimeout)
	}
	result := &streamCloseResult{done: make(chan struct{})}
	s.closeResult = result

	s.stateLock.Unlock()
	queuedCancel := false
	err := s.sendClose(func() {
		queuedCancel = true
		s.stateLock.Lock()
		if !s.terminal {
			if s.state == provisionalState {
				s.state = previousState
			}
		}
		s.closePending = false
		if s.closeTimer != nil {
			s.closeTimer.Stop()
			s.closeTimer = nil
		}
		s.stateLock.Unlock()
	})
	s.notifyWaiting()
	cleanup := false
	keepResult := err != nil && !queuedCancel
	s.stateLock.Lock()
	if !queuedCancel && err == nil {
		s.closePending = false
		if !s.terminal && s.state == streamRemoteClose {
			s.state = streamClosed
			s.terminal = true
			if s.closeTimer != nil {
				s.closeTimer.Stop()
				s.closeTimer = nil
			}
			cleanup = true
		}
	}
	result.err = err
	if !keepResult {
		s.closeResult = nil
	}
	close(result.done)
	s.stateLock.Unlock()
	if cleanup {
		s.session.closeStreamIfOwned(s)
	}
	return err
}

// Reset aborts the stream and discards unread data. Subsequent Read and Write
// calls (including empty calls) return ErrConnectionReset. It wakes blocked
// readers and writers and cancels queued sends. A send already committed to
// the underlying connection still waits for its actual result; an overlapping
// Read or Write may therefore return bytes transferred before the reset.
//
// Reset waits for the RST frame to be written, not for peer acknowledgement.
// ConnectionWriteTimeout bounds queueing, but a committed RST waits for the
// underlying Write to finish. A queue timeout returns ErrConnectionWriteTimeout,
// session shutdown before commitment returns ErrSessionShutdown, and a committed
// write failure returns the transport error. The stream stays reset on failure.
//
// Concurrent and repeated Reset calls share one result, including failures;
// they never retry the RST. Reset on an already fully closed or remotely reset
// stream is a no-op returning nil. Reset may abort a half-closed stream. A
// concurrent Close keeps the result of its own FIN attempt, including a
// retained transport failure on repeated Close calls. Close after reset is
// otherwise a no-op. Session shutdown never changes a reset into an EOF.
func (s *Stream) Reset() error {
	s.stateLock.Lock()
	if result := s.resetResult; result != nil {
		s.stateLock.Unlock()
		<-result.done
		return result.err
	}
	if s.terminal || s.state == streamClosed || s.state == streamReset {
		s.stateLock.Unlock()
		return nil
	}
	result := &streamCloseResult{done: make(chan struct{})}
	s.resetResult = result
	s.resetLocked()
	close(s.resetCh)
	s.stateLock.Unlock()
	s.notifyWaiting()
	result.err = s.session.sendResetIfOwned(s)
	close(result.done)
	return result.err
}

// resetLocked publishes the terminal state while holding stateLock. No
// transport reads may hold recvLock, so discarding data cannot block on a peer.
func (s *Stream) resetLocked() {
	s.state = streamReset
	s.terminal = true
	s.closePending = false
	s.openInFlight = false
	s.openPending = false
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}
	s.recvLock.Lock()
	s.recvReset = true
	s.recvBuf = nil
	s.recvLock.Unlock()
}

// closeTimeout retains nonblocking, ownership-checked reset admission. If a
// public Reset has won the terminal transition, even an already running timer
// must leave that attempt and its pendingReset reservation to Reset.
func (s *Stream) closeTimeout() {
	s.stateLock.Lock()
	if s.resetResult != nil {
		s.stateLock.Unlock()
		return
	}
	s.forceCloseLocked()
	s.stateLock.Unlock()
	s.notifyWaiting()
	s.session.resetStreamIfOwnedNonblocking(s)
}

// forceClose is used for when the session is exiting
func (s *Stream) forceClose() {
	s.stateLock.Lock()
	s.forceCloseLocked()
	s.stateLock.Unlock()
	s.notifyWaiting()
}

func (s *Stream) forceCloseLocked() {
	s.terminal = true
	s.closePending = false
	if s.state != streamReset {
		s.state = streamClosed
	}
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}
}

// terminatePendingOpen marks a not-yet-established stream terminal. It is
// used when a queued SYN/ACK request is canceled; established or already
// terminal streams are left untouched so a concurrent ACK/RST cannot be
// overwritten.
func (s *Stream) claimPendingOpenCleanup() bool {
	s.stateLock.Lock()
	if !s.openInFlight {
		s.stateLock.Unlock()
		return false
	}
	s.openInFlight = false
	s.openPending = false
	if !s.terminal {
		s.terminal = true
		s.closePending = false
		s.state = streamClosed
		if s.closeTimer != nil {
			s.closeTimer.Stop()
			s.closeTimer = nil
		}
	}
	s.stateLock.Unlock()
	s.notifyWaiting()
	return true
}

// processFlags is used to update the state of the stream
// based on set flags, if any. Lock must be held
func (s *Stream) processFlags(flags uint16) error {
	s.stateLock.Lock()
	if s.terminal {
		s.stateLock.Unlock()
		return nil
	}

	// Apply state transitions while holding stateLock, then perform session map
	// ownership changes after releasing it to avoid lock inversion with
	// Session.Close.
	closeStream := false
	establishStream := false

	if flags&flagACK == flagACK {
		if s.state == streamSYNSent && !s.openPending {
			s.state = streamEstablished
			s.openInFlight = false
			establishStream = true
			asyncNotify(s.establishCh)
		}
	}
	if flags&flagFIN == flagFIN {
		switch s.state {
		case streamSYNSent:
			fallthrough
		case streamSYNReceived:
			fallthrough
		case streamEstablished:
			s.state = streamRemoteClose
			s.notifyWaiting()
		case streamLocalClose:
			if s.closePending {
				s.state = streamRemoteClose
				s.notifyWaiting()
			} else {
				s.state = streamClosed
				s.terminal = true
				closeStream = true
				s.notifyWaiting()
			}
		default:
			s.session.logger.Printf("[ERR] yamux: unexpected FIN flag in state %d", s.state)
			s.stateLock.Unlock()
			return ErrUnexpectedFlag
		}
	}
	if flags&flagRST == flagRST {
		s.resetLocked()
		closeStream = true
		s.notifyWaiting()
	}
	if closeStream && s.closeTimer != nil {
		// Stop our close timeout timer since we gracefully closed.
		s.closeTimer.Stop()
		s.closeTimer = nil
	}
	s.stateLock.Unlock()
	if establishStream {
		s.session.establishStream(s)
	}
	if closeStream {
		s.session.closeStreamIfOwned(s)
	}
	return nil
}

// notifyWaiting notifies all the waiting channels
func (s *Stream) notifyWaiting() {
	asyncNotify(s.recvNotifyCh)
	asyncNotify(s.sendNotifyCh)
	asyncNotify(s.establishCh)
}

// incrSendWindow updates the size of our send window
func (s *Stream) incrSendWindow(hdr header, flags uint16) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Increase window, unblock a sender
	atomic.AddUint32(&s.sendWindow, hdr.Length())
	asyncNotify(s.sendNotifyCh)
	return nil
}

// readData is used to handle a data frame
func (s *Stream) readData(hdr header, flags uint16, conn io.Reader) error {
	// Publish a data-frame FIN only after its payload is buffered, so a reader
	// cannot observe EOF while that payload is still arriving.
	if err := s.processFlags(flags &^ flagFIN); err != nil {
		return err
	}

	// Check that our recv window is not exceeded
	length := hdr.Length()
	if length == 0 {
		return s.processFlags(flags & flagFIN)
	}

	// Admit the frame under stateLock so a close either observes an in-flight
	// receive or prevents this frame from publishing bytes after EOF.
	s.stateLock.Lock()
	s.recvLock.Lock()
	if s.terminal {
		s.recvLock.Unlock()
		s.stateLock.Unlock()
		// Preserve a transport error even if its final Read fills the frame.
		copied, err := io.Copy(io.Discard, &io.LimitedReader{R: conn, N: int64(length)})
		if err == nil && copied != int64(length) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if length > s.recvWindow {
		s.session.logger.Printf("[ERR] yamux: receive window exceeded (stream: %d, remain: %d, recv: %d)", s.id, s.recvWindow, length)
		s.recvLock.Unlock()
		s.stateLock.Unlock()
		return ErrRecvWindowExceeded
	}

	// Read directly into reusable, unpublished buffer capacity. recvLoop is
	// the only buffer writer; concurrent Read calls advance only its offset.
	// Neither Read nor Shrink may reset/reuse this tail while it is reserved.
	if s.recvBuf == nil {
		s.recvBuf = new(bytes.Buffer)
	}
	buf := s.recvBuf
	buf.Grow(int(length))
	data := buf.AvailableBuffer()[:length]
	s.recvInFlight = true
	s.recvLock.Unlock()
	s.stateLock.Unlock()

	// Unlike io.ReadFull, retain a non-EOF error returned with the final bytes.
	// Each Read is limited to this frame, leaving the next header untouched.
	var copiedLength int
	var err error
	for copiedLength < len(data) && err == nil {
		var n int
		n, err = conn.Read(data[copiedLength:])
		copiedLength += n
	}
	if err == io.EOF {
		err = nil
		if copiedLength != len(data) {
			err = io.ErrUnexpectedEOF
		}
	}
	if err != nil {
		s.session.logger.Printf("[ERR] yamux: Failed to read stream data: %v", err)
	}

	s.recvLock.Lock()
	if !s.recvReset {
		// Extend the buffer over its own reserved tail: source and destination
		// are identical, so no temporary payload allocation or copy is needed.
		_, _ = buf.Write(data[:copiedLength])
		// Retain partial bytes on transport failure as before; only a reset
		// discards them. Failed frames never consume receive-window credit.
		if err == nil {
			s.recvWindow -= uint32(copiedLength)
		}
	}
	s.recvInFlight = false
	s.recvLock.Unlock()
	// Also wake readers on failure: a closed stream may now return its partial
	// bytes followed by EOF, even when no more DATA will arrive.
	asyncNotify(s.recvNotifyCh)
	if err != nil {
		return err
	}

	return s.processFlags(flags & flagFIN)
}

// SetDeadline sets the read and write deadlines
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// SetReadDeadline sets the deadline for blocked and future Read calls.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetWriteDeadline sets the deadline for blocked and future Write calls
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	asyncNotify(s.sendNotifyCh)
	return nil
}

// Shrink is used to compact the amount of buffers utilized
// This is useful when using Yamux in a connection pool to reduce
// the idle memory utilization.
func (s *Stream) Shrink() {
	s.recvLock.Lock()
	if !s.recvInFlight && s.recvBuf != nil && s.recvBuf.Len() == 0 {
		s.recvBuf = nil
	}
	s.recvLock.Unlock()
}
