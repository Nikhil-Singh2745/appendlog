# Format and recovery

## Bytes on disk

All integers are little-endian. No padding or alignment between records.

The file starts with 16 bytes:

| Offset | Bytes | Value |
| --- | --- | --- |
| 0 | 8 | `APNDLOG\0` |
| 8 | 4 | version, currently `1` |
| 12 | 4 | reserved, all zero |

Each record starts with 12 bytes:

| Offset within record | Bytes | Value |
| --- | --- | --- |
| 0 | 4 | payload length, at most 1,048,576 |
| 4 | 4 | CRC-32 of the payload |
| 8 | 4 | CRC-32 of the preceding eight header bytes |
| 12 | length | payload |

CRC uses the IEEE polynomial, reflected representation `0xedb88320`, with an
initial value of `0xffffffff` and a final XOR of `0xffffffff`. `123456789` gives
`0xcbf43926`. An empty payload has CRC zero but still takes 12 bytes for its header.

The frame for `hello` is:

```text
05 00 00 00  86 a6 10 36  f0 9e 7d bd  68 65 6c 6c 6f
length       payload CRC  header CRC   payload
```

Its offset is 16 in an otherwise empty log. The next record starts at 33. The
fixed frame in the tests was cross-checked using Python's `struct` and `zlib`.

I put a checksum on the header because an unchecked damaged length can look like
an incomplete payload. Header validation comes before trusting that length or
allocating memory. CRC is for accidental damage; it isn't authentication.

## Opening and recovery

A writer opens without truncation and acquires an exclusive nonblocking lock. A
reader opens read-only and acquires a shared lock. Both validate the file header
and scan from byte 16.

| Observation | Reader | Writer |
| --- | --- | --- |
| EOF between records | complete prefix | preserve |
| 1–11 final bytes | report incomplete header | discard tail |
| valid header, too few payload bytes | report incomplete payload | discard tail |
| complete header with bad CRC | error | error, preserve file |
| length over the limit, even with valid CRC | error | error, preserve file |
| complete payload with bad CRC | error | error, preserve file |
| incomplete or unsupported file header | error | error, preserve file |

Recovery only truncates after the scan succeeds. A damaged earlier record plus a
partial tail remains untouched. The repair removes the entire incomplete record,
including its header, then syncs the file. The next append starts at the repaired
boundary. A reader reports the tail but only yields complete records.

The tail rules assume an interrupted append. They can't distinguish one from
arbitrary truncation. A file shortened exactly to a record boundary looks valid.
There is no external acknowledgment ledger to detect lost complete records, and
no sequence field to detect reordered or duplicated checksum-valid frames.

An interrupted initial header is rejected. I don't recreate it automatically:
an empty or short existing file might be unrelated data. Initialization errors
can therefore leave an invalid file that needs inspection before removal.

## Acknowledgment

The writer has mutable ownership of one file handle and writes directly through
`File`; there's no userspace buffer to flush.

```text
Always: header -> payload -> sync_all -> return offset
Manual: header -> payload -> return offset
sync(): sync_all -> return success
```

Opening a writer also syncs the validated prefix and the parent directory. This
covers new-file directory metadata and retrying an earlier failed initialization
after its complete header reached the filesystem. The parent must already exist;
creation and syncing of ancestor directories is outside this API.

`Always` ties acknowledgment to a successful filesystem sync call. The actual
durability still depends on the filesystem and storage stack honoring that call.
This project doesn't simulate reordered persistence or test sudden power loss.

`Manual` can batch several appends behind one explicit barrier. A successful
write generally leaves bytes in the kernel, which is different from committing
them to stable storage. Neither `Drop` nor the manual CLI append adds a barrier.

After any write or sync error, the writer stops accepting operations. Continuing
could overwrite a partial frame using an offset that no longer describes the
file. Reopening rescans it instead.

A sync failure is ambiguous: all bytes may be present even though append returned
an error. Recovery keeps a complete checksum-valid record and syncs it. The caller
must handle duplicate retries if that matters to the application. Multi-record
transactions and exactly-once delivery aren't provided here.

## Group commit

I kept the single-file writer and put the batching policy above it. One worker
owns a `Manual` writer; producers only own queue senders. This keeps file offsets
and poison handling in one place. No async runtime or additional dependencies.

```text
submit A -> queue -> append A --+
submit B -> queue -> append B --+-> sync_all -> reply A, reply B
```

The first dequeued request starts a collection window. The worker stops collecting
at the record cap, the window's deadline, or a shutdown marker. It doesn't reset
the deadline when another record arrives, so a steady producer can't postpone
the barrier forever. An idle worker waits without syncing. A zero window removes
the deliberate wait; an already queued request may still join the batch.

The defaults cap queued payloads at 128 MiB and the batch at 64 MiB, before
allocation and channel overhead. Each blocked submitting thread can also hold
one copied payload outside the queue. Receipts retained by callers consume memory
too. This bounds the worker's payload backlog, not total process memory; producer
counts and retained receipts still need an application-level limit.

Submission and acknowledgment are separate. `submit` only transfers work to the
queue; `Commit::wait` returns an offset after sync succeeds. Batches share a
barrier, not a transaction: a failed batch may leave a valid prefix, or even all
its records, on disk. Dropping a receipt doesn't remove its request.

An append or sync failure rejects every receipt in the current batch with the
same storage error, including records not attempted yet. The worker exits instead
of continuing after ambiguous I/O. Queued receipts then receive `Closed`; an
unexpected worker panic has the same effect on receipts. Explicit shutdown also
reports the worker's storage error or panic. Retry still requires deduplication
outside this format.

Shutdown uses a marker in the same queue. Requests ahead of it finish; requests
behind it are dropped without acknowledgment. A submission racing shutdown may
enqueue successfully and then receive `Closed`, so producers should finish first.
The owner joins its worker, ensuring the file lock is released even if appender
clones remain alive. `Drop` does the same work and can block on disk I/O, but discards
the result. `shutdown` is the path for observing errors.

## What the tests establish

The recovery tests cut the final record at every byte boundary, inspect without
mutation, reopen, and append at the recovered boundary. Corruption tests change
both header and payload bytes, including the last complete record.

An internal fault sink wraps a real file. It limits successful write bytes,
injects interrupted or short writes, and fails sync calls. The same append code
runs with this sink and with ordinary files; the wrapper isn't part of the public
API. These tests check poisoning, barrier placement, and reopen behavior.

CLI tests run another process while a parent handle holds a lock. This checks
that exclusion isn't just enforced by mutable Rust references.

The process-crash tests kill a writer with SIGKILL after it reports three records
past a sync barrier: per-record sync with `Always`, an explicit batch sync with
`Manual`, or successful group-commit receipts. The parent checks exclusion before
the kill, then verifies the exact records and offsets, no discarded tail, and an
append at the recovered boundary.
A pipe handshake fixes the kill point; it doesn't depend on a sleep. This covers
lock release and reopening after process death. The kernel stays running, so it
doesn't establish power-loss durability or exercise a kill halfway through a write.

The group worker also runs against a private test sink. One test holds its sync
barrier blocked and checks that all receipts remain pending. Others count syncs
across bounded batches and inject append or sync failures. Integration tests use
four concurrent producers with a queue smaller than their combined workload,
then compare every acknowledged offset and payload with the file in order.

There are no benchmark numbers yet. The next useful performance question is the
cost of one sync per append against explicit batches, using a stated filesystem,
payload distribution, and batch size.

## Segments

I used a separate directory API rather than changing what existing byte offsets
mean. `LogPosition` is `(segment_id, offset)`, ordered by segment then byte. A
segment is an ordinary v1 file; IDs live in canonical 20-digit filenames. Segment
zero starts at byte 16, just like a standalone log.

```text
LOCK
00000000000000000000.alog   sealed
00000000000000000001.alog   active
.pending                   unpublished, if present
```

The highest contiguous ID is active. There's no manifest to update, so an empty
published highest segment is a valid state. It still needs the complete valid
file header. Gaps, unknown entries, symlinks, and directories in reserved paths
are errors. A writer creates `LOCK` if needed; a failed first open can leave that
lock file behind. Readers never create it.

The directory must already exist. I don't create or sync its parent; directory
creation belongs to the caller. A canonical absolute path keeps later rotations
independent of the process working directory. Paths and reserved files still
must not be replaced while handles are open.

### Rotation ordering

```text
sync old segment
create .pending exclusively
write 16-byte header
sync .pending
rename .pending -> next ID
sync directory
append triggering frame
sync active segment if Always
return position
```

Once another segment is published, the old one must be a complete, synced prefix.
The directory barrier comes before any records are written into the new file.
`Manual` therefore gets an extra barrier on rotation, but doesn't acknowledge the
triggering record as durable. `sync()` only needs the active file because every
older segment crossed a rotation barrier already.

| Interrupted state | Reopen behavior |
| --- | --- |
| No published segments, partial staging | discard staging, initialize zero |
| Old segment plus staging | preserve old, discard staging |
| New name published with a valid empty header | accept it as active |
| Active segment has an incomplete final record | discard that record |
| Older segment has an incomplete record | reject; don't repair it |
| Any published header or complete record is corrupt | reject; preserve bytes |

Writer open validates the entire published set before removing staging or
truncating a tail. It then syncs all published files and the directory before
returning, including on retry after a previous initialization error. Staging is
never scanned for records: by construction, no append reaches an unpublished
file. A write, sync, or publication error poisons the owning writer.

The size limit includes the file header. Rotation happens before exceeding it,
except that a single valid record may exceed the limit in an empty segment. This
avoids repeatedly rotating a large frame without making progress. Changing the
limit on reopen doesn't rewrite old segments or rotate before the next append.

### Evidence and limits

Private observation points run before and after the real publication operations.
Tests inject errors at every boundary, check poisoning, and reopen the resulting
files. Another test leaves every possible partial staged header. An ordering
trace checks that the new file still contains only its header after directory
sync, before the triggering frame is written. These are injected boundary errors,
not a device simulator.

A subprocess test parks the real writer after the staged-header sync, after
rename, after directory sync, and after a successful `Always` append. The parent
uses a pipe handshake, checks lock exclusion, kills it, and verifies the exact
acknowledged records and restart positions. The pre-append states have one old
record; the acknowledged state also has the new record. These are process-crash
tests: the kernel remains alive, including its page cache and directory state.

Readers hold a shared directory lock and open one segment at a time. They scan
all segments before returning from open, then read them again. Metadata grows
with segment count, payload memory with the largest frame. Direct access through
the single-file API bypasses the directory lock and is outside this contract.

There is no durable external inventory or ID in a segment's header. Removing the
last segment, truncating at a valid record boundary, or swapping checksum-valid
segments can go undetected. Contiguous names detect internal gaps, not arbitrary
data loss. Retention will need an explicit base ID and stronger metadata rather
than relaxing the gap check.

## Next slice

Let the group worker use a segmented writer and return `LogPosition` receipts,
while preserving the current single-file API. A batch that crosses rotation has
multiple barriers; tests should pin down which failures leave receipts ambiguous
before comparing its throughput with the single-file path.
