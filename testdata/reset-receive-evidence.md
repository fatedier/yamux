Receive regression evidence, 2026-09-10

Measured on Go 1.26.7, darwin/arm64, Apple M5 Pro. The baseline is the exact
`0e5d0477ac08126e1e2ea80279f5513a7088d1fb` tree. “Before” is the Reset API
worktree entering this fix; “after” includes in-flight receive tracking and
direct reception into reserved receive-buffer capacity. No git refs were
changed for the comparisons. These measurements are allocation evidence,
not a fresh review verdict or completion of the full gate suite.

`BenchmarkSendRecvLarge` transfers 512 MiB per operation. Results below are
three sequential runs with `-benchtime=1x -count=3`, with the same Go binary:

| Tree | B/op, run 1 | B/op, run 2 | B/op, run 3 | Median B/op |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 4,072,960 | 4,085,480 | 4,025,704 | 4,072,960 |
| Before | 1,428,143,560 | 1,424,337,184 | 1,415,381,880 | 1,424,337,184 |
| After | 4,551,512 | 4,643,064 | 4,651,960 | 4,643,064 |

`BenchmarkStreamReceiveBuffered` isolates DATA reception with one unread byte
retained between frames. It reuses the input reader and application storage,
and excludes network and send-queue allocations. Median results from three
runs of 1,000 frames each:

| DATA size | Baseline B/op; allocs/op | Before B/op; allocs/op | After B/op; allocs/op |
| --- | ---: | ---: | ---: |
| 32 KiB | 24; 1 | 98,377; 4 | 0; 0 |
| 128 KiB | 24; 1 | 393,292; 4 | 0; 0 |

Reproduce the benchmarks from each checkout (copy the standalone
`stream_receive_bench_test.go` into the baseline when comparing):

```sh
go test -run '^$' -bench '^BenchmarkSendRecvLarge$' -benchmem -benchtime=1x -count=3 -timeout=90s
go test -run '^$' -bench '^BenchmarkStreamReceiveBuffered$' -benchmem -benchtime=1000x -count=3 -timeout=90s
```

Before the implementation change, the new deterministic tests reproduced
premature EOF in all four partial-frame close cases (timeout/forceClose,
with/without previously buffered data) and both partial-frame error cases.
`TestStreamReceiveBufferedAllocations` also failed at 4 allocations per frame.
After the change those tests pass. They verify that the tail becomes readable
before EOF, a failed frame wakes the reader and retains partial bytes, late
DATA cannot publish bytes after EOF, and the next stream remains correctly
framed. The existing `TestStreamResetDuringDataFrame` still verifies that Reset
finishes while the body is stalled, discards unread data, and restores framing
when the remaining body arrives. The new Shrink interleaving verifies that
reading published bytes and shrinking cannot invalidate a reserved tail.

The focused receive/Reset/lifecycle suite passed with the race detector and
`-count=20`. Ordinary send/receive, many-stream traffic, large windows, and
window-update tests also passed with `-race -short`. Formatting, vet, build,
and lint passed. Full gates and fresh `/review` remain the subsequent workflow.

```sh
go test -race -run 'TestStreamReceive|TestStreamReset|TestStreamReadData|TestStreamCloseTimeout|TestHalfClose|TestSessionClose|TestAcceptStream' -count=20 -timeout=120s
go test -race -short -run 'TestSendData|TestManyStreams|TestLargeWindow|TestSession_.*WindowUpdate' -count=1 -timeout=120s
```

Compared `stream.go` SHA-256 values:

- Before: `3d7a61032db455a7e7020dbbc7f0afa3b3d917f661d8ecad70169676485d7c8e`
- After: `77c489d7e0f3f26a05110a4a241ec43c36ce684a65f76f8579f2ee39ba818c7e`
