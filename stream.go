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
	// openInFlight tracks ownership of a stream that is not yet established.
	// openPending is true only while its SYN/ACK request is still queued.
	openInFlight bool
	openPending  bool

	recvBuf  *bytes.Buffer
	recvLock sync.Mutex

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
// Close concurrently with each other, but calls to Read are not reentrant and
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
		if s.recvBuf == nil || s.recvBuf.Len() == 0 {
			s.recvLock.Unlock()
			s.stateLock.Unlock()
			return 0, io.EOF
		}
		s.recvLock.Unlock()
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	s.recvLock.Lock()
	if s.recvBuf == nil || s.recvBuf.Len() == 0 {
		s.recvLock.Unlock()
		goto WAIT
	}

	// Read any bytes
	n, _ = s.recvBuf.Read(b)
	s.recvLock.Unlock()

	// Send a window update potentially
	err = s.sendWindowUpdate()
	if err != nil && s.session.isShuttingDown() {
		// A failed update can be the event that terminates this session. The
		// bytes were already delivered from recvBuf, so preserve the read result
		// while keeping ordinary request errors visible to callers.
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
// Close concurrently with each other, but calls to Write are not reentrant and
// should not be called from multiple goroutines.
func (s *Stream) Write(b []byte) (n int, err error) {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
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
	if err = s.session.waitForSendErrWithHooks(s.sendHdr, body, flagsRollback); err != nil {
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
	if err := s.session.waitForSendErrWithCommit(s.controlHdr, nil, rollback, onCommit); err != nil {
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
	if err := s.session.waitForSendErrWithCommit(s.controlHdr, nil, rollback, onCommit); err != nil {
		return err
	}
	if flags&flagACK != 0 {
		s.stateLock.Lock()
		s.openInFlight = false
		s.stateLock.Unlock()
	}
	return nil
}

// Close is used to close the stream. It is safe to call Close concurrently.
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

// closeTimeout is called after StreamCloseTimeout during a close to
// close this stream.
func (s *Stream) closeTimeout() {
	// Close our side forcibly and bind reset queueing to exact registry
	// ownership. A stale timer must not reset a replacement with the same ID.
	s.forceClose()

	// Send a RST so the remote side closes too.
	s.session.resetStreamIfOwnedNonblocking(s)
}

// forceClose is used for when the session is exiting
func (s *Stream) forceClose() {
	s.stateLock.Lock()
	s.terminal = true
	s.closePending = false
	s.state = streamClosed
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}
	s.stateLock.Unlock()
	s.notifyWaiting()
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
		s.state = streamReset
		s.terminal = true
		s.closePending = false
		s.openInFlight = false
		s.openPending = false
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
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Check that our recv window is not exceeded
	length := hdr.Length()
	if length == 0 {
		return nil
	}

	// Limit the copy to this frame so bytes belonging to the next frame remain
	// buffered for recvLoop. io.Copy preserves a non-EOF reader error even when
	// the final read supplies the complete frame.
	limited := &io.LimitedReader{R: conn, N: int64(length)}

	// Copy into buffer
	s.recvLock.Lock()

	if length > s.recvWindow {
		s.session.logger.Printf("[ERR] yamux: receive window exceeded (stream: %d, remain: %d, recv: %d)", s.id, s.recvWindow, length)
		s.recvLock.Unlock()
		return ErrRecvWindowExceeded
	}

	if s.recvBuf == nil {
		// Allocate the receive buffer just-in-time to fit the full data frame.
		// This way we can read in the whole packet without further allocations.
		s.recvBuf = bytes.NewBuffer(make([]byte, 0, length))
	}
	copiedLength, err := io.Copy(s.recvBuf, limited)
	if err != nil {
		s.session.logger.Printf("[ERR] yamux: Failed to read stream data: %v", err)
		s.recvLock.Unlock()
		return err
	}
	if copiedLength != int64(length) {
		s.recvLock.Unlock()
		return io.ErrUnexpectedEOF
	}

	// Decrement the receive window
	s.recvWindow -= uint32(copiedLength)
	s.recvLock.Unlock()

	// Unblock any readers
	asyncNotify(s.recvNotifyCh)
	return nil
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
	if s.recvBuf != nil && s.recvBuf.Len() == 0 {
		s.recvBuf = nil
	}
	s.recvLock.Unlock()
}
