Test certificates generated with:

```
go run $(go env GOROOT)/src/crypto/tls/generate_cert.go --host example.com
```

Requires a bash-like shell and Go installed.

Reset interoperability is checked against actual historical source, using a
temporary module and local replacements (no downloads or git ref changes):

```sh
testdata/reset-interop/run.sh                # pre-lifecycle-change c38d75f
testdata/reset-interop/run.sh 0e5d0477        # reviewed lifecycle baseline
```

The script runs with the race detector and covers client/server roles, reset
before accept, established and half-closed streams, unread data, and continued
bidirectional use of the session. Pass additional `go test` flags after the
revision. The normal unit suite also tests exact version-0 RST bytes, late
frames, failure handling, and deterministic concurrency using `testing/synctest`.
