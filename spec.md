# Specification

We use this document to detail the internal specification of Yamux.
This is used both as a guide for implementing Yamux, but also for
alternative interoperable libraries to be built.

# Framing

Yamux uses a streaming connection underneath, but imposes a message
framing so that it can be shared between many logical streams. Each
frame contains a header like:

* Version (8 bits)
* Type (8 bits)
* Flags (16 bits)
* StreamID (32 bits)
* Length (32 bits)

This means that each header has a 12 byte overhead.
All fields are encoded in network order (big endian).
Each field is described below:

## Version Field

The version field is used for future backward compatibility. At the
current time, the field is always set to 0, to indicate the initial
version.

## Type Field

The type field is used to switch the frame message type. The following
message types are supported:

* 0x0 Data - Used to transmit data. May transmit zero length payloads
  depending on the flags.

* 0x1 Window Update - Used to updated the senders receive window size.
  This is used to implement per-session flow control.

* 0x2 Ping - Used to measure RTT. It can also be used to heart-beat
  and do keep-alives over TCP.

* 0x3 Go Away - Used to close a session.

## Flag Field

The flags field is used to provide additional information related
to the message type. The following flags are supported:

* 0x1 SYN - Signals the start of a new stream. May be sent with a data or
  window update message. Also sent with a ping to indicate outbound.

* 0x2 ACK - Acknowledges the start of a new stream. May be sent with a data
  or window update message. Also sent with a ping to indicate response.

* 0x4 FIN - Performs a half-close of a stream. May be sent with a data
  message or window update.

* 0x8 RST - Reset a stream immediately. May be sent with a data or
  window update message.

## StreamID Field

The StreamID field is used to identify the logical stream the frame
is addressing. The client side should use odd ID's, and the server even.
This prevents any collisions. Additionally, the 0 ID is reserved to represent
the session.

Both Ping and Go Away messages should always use the 0 StreamID.

## Length Field

The meaning of the length field depends on the message type:

* Data - provides the length of bytes following the header
* Window update - provides a delta update to the window size
* Ping - Contains an opaque value, echoed back
* Go Away - Contains an error code

# Message Flow

There is no explicit connection setup, as Yamux relies on an underlying
transport to be provided. However, there is a distinction between client
and server side of the connection.

## Opening a stream

To open a stream, an initial data or window update frame is sent
with a new StreamID. The SYN flag should be set to signal a new stream.

The receiver must then reply with either a data or window update frame
with the StreamID along with the ACK flag to accept the stream or with
the RST flag to reject the stream.

Because we are relying on the reliable stream underneath, a connection
can begin sending data once the SYN flag is sent. The corresponding
ACK does not need to be received. This is particularly well suited
for an RPC system where a client wants to open a stream and immediately
fire a request without waiting for the RTT of the ACK.

This does introduce the possibility of a connection being rejected
after data has been sent already. This is a slight semantic difference
from TCP, where the connection cannot be refused after it is opened.
Clients should be prepared to handle this by checking for an error
that indicates a RST was received.

## Closing a stream

To close a stream, either side sends a data or window update frame
along with the FIN flag. This does a half-close indicating the sender
will send no further data.

Once both sides have closed the connection, the stream is closed.

Alternatively, if an error occurs, the RST flag can be used to
hard close a stream immediately.

### Resetting a stream

`(*Stream).Reset() error` aborts both directions locally, discards unread
data, removes the stream from the session, and sends one version-0 Window
Update with flags `0x8` (RST), the stream ID, and length zero. It does not
send FIN, SYN, or ACK with the RST. No reset acknowledgement exists in the
protocol, and other streams remain usable.

After the local reset transition, Read and Write return
`ErrConnectionReset`, including calls with empty buffers. Readers waiting
for data and writers waiting for flow-control credit wake up. Queued data,
window updates, and FINs are canceled before transport commitment. A Read
that already copied data may return those bytes with an error; a Write
whose frame has already committed waits for that frame's actual transport
result and may return a successful or partial write. Reset cannot retract
bytes already submitted to the connection or consumed by the peer.

Reset returns nil after the underlying connection accepts the complete RST
header. This does not prove that the peer has processed it. Failure to queue
or commit before `ConnectionWriteTimeout` returns
`ErrConnectionWriteTimeout`; session shutdown before commitment returns
`ErrSessionShutdown`. After commitment, Reset waits for the underlying
Write and returns its error, including `io.ErrShortWrite`. Stream read/write
deadlines do not govern Reset. A committed write therefore needs the
underlying transport to complete or be interrupted by session closure.

The local transition is irreversible even when sending RST fails.
Concurrent and repeated Reset calls share the same attempt and result,
including failures, without retrying or sending duplicate RSTs. A failed
RST can leave the peer unaware of the reset; the caller can close the
session if it needs to terminate that connection too.

Reset can abort either half-closed state. A concurrent Close retains the
result of its FIN attempt: a queued FIN canceled by Reset returns
`ErrConnectionReset`, while a committed FIN reports its transport result.
Calls joining that Close share its result; a committed FIN failure remains
the result of repeated Close calls, even after reset. Otherwise Close after
reset returns nil. The existing close timeout remains nonblocking: it
force-closes locally and admits its ownership-checked RST asynchronously.
If the timeout wins first, Reset is a no-op preserving that terminal state.
If public Reset wins first, the timer leaves its send attempt untouched.

On a fully closed or remotely reset stream, Reset returns nil without
sending anything or changing the existing terminal state. Thus a completed
graceful close or session teardown preserves buffered reads followed by
EOF and `ErrStreamClosed` for nonempty writes. If Reset wins the terminal
transition first, subsequent session teardown preserves
`ErrConnectionReset` and does not restore discarded data. If shutdown
starts before Reset but has not closed the stream yet, Reset can still
win locally and return `ErrSessionShutdown` for the unsent RST.

Reset removes only the exact stream it owns. A `pendingReset` reservation
protects that ID until its send attempt completes or is canceled; a SYN for
the same ID is ignored during this interval. Sending never holds the session
registry lock, so unrelated streams and session teardown can proceed. A
stale stream handle cannot reset or remove a replacement. Accept's shutdown
checks and Session.Close's cleanup of selected and queued streams still
apply, and their forced closes preserve an existing reset state.

### Compatibility with older peers

Reset uses the existing version-0 RST flag; it requires no negotiation,
protocol version change, or matching public API on the peer. It can be
sent after SYN and before ACK, or after a half-close. Local pending-SYN
credit is released exactly once. An incoming stream already in the accept
queue may still be returned by AcceptStream after receiving RST; its Read
and Write report `ErrConnectionReset`.

Receivers accept RST in either Data or Window Update frames. A Data frame's
payload must still be consumed to preserve framing, even though its data
is discarded. In-flight data and late ACK, FIN, or RST for a removed stream
are drained or ignored. They must not close unrelated streams.

Compatibility is at the wire level. Older implementations need not provide
the same local cancellation, buffer reclamation, idempotency, or error
precedence during simultaneous session shutdown. In particular, versions
whose session teardown overwrites reset state can report EOF or
`ErrStreamClosed` instead of `ErrConnectionReset` in that race. Reset cannot
guarantee delivery when queueing or the underlying connection fails.

## Flow Control

When Yamux is initially starts each stream with a 256KB window size.
There is no window size for the session.

To prevent the streams from stalling, window update frames should be
sent regularly. Yamux can be configured to provide a larger limit for
windows sizes. Both sides assume the initial 256KB window, but can
immediately send a window update as part of the SYN/ACK indicating a
larger window.

Both sides should track the number of bytes sent in Data frames
only, as only they are tracked as part of the window size.

## Session termination

When a session is being terminated, the Go Away message should
be sent. The Length should be set to one of the following to
provide an error code:

* 0x0 Normal termination
* 0x1 Protocol error
* 0x2 Internal error
