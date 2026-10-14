# appendlog

A small Rust append log. I wanted to work through the acknowledgment and recovery
rules before adding segments or a service around it.

The current version is a library and CLI over one file. Records carry a bounded
length and checksums for both the header and payload. Opening a writer validates
the complete log, then removes an incomplete final record. Checksum failures are
errors; they don't trigger repair.

## Run

Rust 1.89 or later, on Linux. There are no dependencies outside `std`.

```sh
mkdir -p scratch
cargo run --offline -- init scratch/events.alog
cargo run --offline -- append scratch/events.alog 'hello'
cargo run --offline -- append scratch/events.alog 'world'
cargo run --offline -- read scratch/events.alog
cargo run --offline -- inspect scratch/events.alog
```

`read` prints byte offsets and hex payloads:

```text
16	68656c6c6f
33	776f726c64
```

`inspect` reports the format version, record count, total size, validated prefix,
and any incomplete tail. Neither read command changes the file. Both validate the
whole file before producing records, so corruption later in the log fails the
command before it prints a partial result.

`init` is also a reopen operation: an existing valid file is preserved, and an
incomplete tail is repaired. `append` creates a missing file. Parent directories
must already exist. Invalid command syntax exits with status 2; storage or output
errors exit with status 1.

## Library

```rust
use appendlog::{Reader, SyncPolicy, Writer};

fn main() -> Result<(), appendlog::Error> {
    let (mut writer, recovery) = Writer::open("events.alog", SyncPolicy::Always)?;
    let offset = writer.append(b"hello")?;
    println!("offset={offset}, discarded={}", recovery.discarded_bytes);
    drop(writer);

    let mut reader = Reader::open("events.alog")?;
    while let Some(record) = reader.next_record()? {
        println!("{}: {:?}", record.offset, record.payload);
    }
    Ok(())
}
```

The library accepts arbitrary bytes, including empty payloads, up to 1 MiB per
record. The CLI appends UTF-8 text. Offsets are positions in the file, not sequence
numbers or deduplication keys.

## Durability

`Always`, the default CLI policy, returns success after writing the full record
and calling `sync_all`. With `Manual`, append returns after writing; `Writer::sync`
establishes a barrier for preceding records. Dropping a writer does not sync it.

```sh
cargo run --offline -- append scratch/events.alog 'unsynced' --sync manual
```

That command intentionally ends without a final sync for the appended record.
Closing the process is not a durability barrier. Every writer open syncs the
validated file and its parent directory before returning.

A write or sync error disables that writer. Close it and reopen to recover. A
failed append can still leave a complete record, so retrying may duplicate it.

## Check

```sh
cargo test --offline
cargo fmt --check
cargo clippy --offline --all-targets -- -D warnings
```

Tests cover record boundaries, corrupt input, partial writes, failed syncs, and
locks across processes. They exercise recovery logic and sync ordering, not power
loss on a real device.

## Limits

One file, one writer, no rotation, index, or group commit. Readers hold shared
locks and exclude writers for their lifetime. Locks are advisory on Linux: other
programs must cooperate, and the path must not be replaced while a handle is open.

Opening either handle scans the whole file. A reader then reads the records again;
memory stays bounded by record size. That's a deliberate starting point, but it
won't scale to large logs.

The [format and recovery notes](notes.md) explain which damaged tails can be
repaired and what the checksums can't establish.
