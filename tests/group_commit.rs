mod support;

use std::{
    fs,
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use appendlog::{
    CommitError, Error, GroupOptions, GroupWriter, MAX_PAYLOAD, Reader, SyncPolicy, Writer,
};
use support::Workspace;

fn read_all(work: &Workspace) -> Vec<(u64, Vec<u8>)> {
    let mut reader = Reader::open(&work.log).unwrap();
    assert!(reader.inspection().tail.is_none());
    let mut records = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        records.push((record.offset, record.payload));
    }
    records
}

#[test]
fn concurrent_producers_get_offsets_in_file_order() {
    let work = Workspace::new();
    let options = GroupOptions {
        queue_capacity: 4,
        max_batch_records: 8,
        ..GroupOptions::default()
    };
    let (writer, _) = GroupWriter::open(&work.log, options).unwrap();
    assert!(matches!(Reader::open(&work.log), Err(Error::Busy)));
    let start = Arc::new(Barrier::new(5));
    let mut producers = Vec::new();
    for producer in 0..4 {
        let appender = writer.appender();
        let start = start.clone();
        producers.push(thread::spawn(move || {
            start.wait();
            let mut pending = Vec::new();
            for sequence in 0..24 {
                let payload = vec![producer, sequence];
                pending.push((appender.submit(&payload).unwrap(), payload));
            }
            pending
                .into_iter()
                .map(|(commit, payload)| (commit.wait().unwrap(), payload))
                .collect::<Vec<_>>()
        }));
    }
    start.wait();
    let mut expected = Vec::new();
    for producer in producers {
        let records = producer.join().unwrap();
        assert!(records.windows(2).all(|pair| pair[0].0 < pair[1].0));
        expected.extend(records);
    }
    writer.shutdown().unwrap();
    expected.sort_unstable_by_key(|record| record.0);
    assert_eq!(read_all(&work), expected);
    assert_eq!(expected.len(), 96);
    assert_eq!(expected[0].0, 16);
    assert!(expected.windows(2).all(|pair| pair[1].0 == pair[0].0 + 14));
}

#[test]
fn shutdown_drains_requests_and_disconnects_surviving_producers() {
    let work = Workspace::new();
    let options = GroupOptions {
        max_delay: Duration::from_secs(60),
        ..GroupOptions::default()
    };
    let (writer, _) = GroupWriter::open(&work.log, options).unwrap();
    let appender = writer.appender();
    let first = appender.submit(b"first").unwrap();
    let second = appender.submit(b"second").unwrap();
    writer.shutdown().unwrap();
    assert_eq!(first.wait().unwrap(), 16);
    assert_eq!(second.wait().unwrap(), 33);
    assert!(matches!(appender.submit(b"late"), Err(CommitError::Closed)));
    assert_eq!(
        read_all(&work),
        [(16, b"first".to_vec()), (33, b"second".to_vec())]
    );
}

#[test]
fn dropping_the_owner_finishes_preceding_requests() {
    let work = Workspace::new();
    let options = GroupOptions {
        max_delay: Duration::from_secs(60),
        ..GroupOptions::default()
    };
    let (writer, _) = GroupWriter::open(&work.log, options).unwrap();
    let appender = writer.appender();
    let commit = appender.submit(b"pending").unwrap();
    drop(writer);
    assert_eq!(commit.wait().unwrap(), 16);
    assert!(matches!(appender.submit(b"late"), Err(CommitError::Closed)));
    assert_eq!(read_all(&work), [(16, b"pending".to_vec())]);
}

#[test]
fn payload_limits_and_zero_delay_leave_a_usable_writer() {
    let work = Workspace::new();
    let options = GroupOptions {
        max_delay: Duration::ZERO,
        ..GroupOptions::default()
    };
    let (writer, _) = GroupWriter::open(&work.log, options).unwrap();
    let appender = writer.appender();
    let oversized = appender.submit(&vec![0; MAX_PAYLOAD + 1]);
    assert!(
        matches!(oversized, Err(CommitError::Storage(error)) if matches!(error.as_ref(), Error::PayloadTooLarge { .. }))
    );
    let binary = vec![0xa5; MAX_PAYLOAD];
    let empty = appender.submit(b"").unwrap();
    let maximum = appender.submit(&binary).unwrap();
    assert_eq!(empty.wait().unwrap(), 16);
    assert_eq!(maximum.wait().unwrap(), 28);
    writer.shutdown().unwrap();
    assert_eq!(read_all(&work), [(16, vec![]), (28, binary)]);
}

#[test]
fn invalid_options_do_not_create_a_log() {
    let work = Workspace::new();
    for options in [
        GroupOptions {
            queue_capacity: 0,
            ..GroupOptions::default()
        },
        GroupOptions {
            max_batch_records: 0,
            ..GroupOptions::default()
        },
    ] {
        assert!(
            matches!(GroupWriter::open(&work.log, options), Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
        );
        assert!(!work.log.exists());
    }
}

#[test]
fn opening_a_group_writer_recovers_a_tail_and_preserves_corruption() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"partial").unwrap();
    drop(writer);
    let mut bytes = fs::read(&work.log).unwrap();
    bytes.truncate(bytes.len() - 2);
    fs::write(&work.log, bytes).unwrap();
    let (writer, recovery) = GroupWriter::open(&work.log, GroupOptions::default()).unwrap();
    assert_eq!(recovery.record_count, 1);
    assert_eq!(recovery.valid_bytes, 33);
    assert_eq!(recovery.discarded_bytes, 17);
    let appender = writer.appender();
    assert_eq!(appender.submit(b"replacement").unwrap().wait().unwrap(), 33);
    writer.shutdown().unwrap();
    assert_eq!(
        read_all(&work),
        [(16, b"first".to_vec()), (33, b"replacement".to_vec())]
    );

    let mut damaged = fs::read(&work.log).unwrap();
    damaged[28] ^= 1;
    fs::write(&work.log, &damaged).unwrap();
    assert!(matches!(
        GroupWriter::open(&work.log, GroupOptions::default()),
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(fs::read(&work.log).unwrap(), damaged);
}
