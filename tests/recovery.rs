mod support;

use std::{fs, fs::OpenOptions};

use appendlog::{Error, MAX_PAYLOAD, Reader, SyncPolicy, TailKind, Writer};
use support::Workspace;

fn read_all(work: &Workspace) -> Vec<(u64, Vec<u8>)> {
    let mut reader = Reader::open(&work.log).unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        records.push((record.offset, record.payload));
    }
    assert!(reader.next_record().unwrap().is_none());
    records
}

#[test]
fn encoding_matches_an_independent_wire_fixture() {
    let work = Workspace::new();
    let bytes = [
        0x41, 0x50, 0x4e, 0x44, 0x4c, 0x4f, 0x47, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x05, 0x00, 0x00, 0x00, 0x86, 0xa6, 0x10, 0x36, 0xf0, 0x9e, 0x7d, 0xbd, 0x68, 0x65,
        0x6c, 0x6c, 0x6f,
    ];
    fs::write(&work.log, bytes).unwrap();
    assert_eq!(read_all(&work), vec![(16, b"hello".to_vec())]);
    fs::write(&work.log, &bytes[..16]).unwrap();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    writer.append(b"hello").unwrap();
    drop(writer);
    assert_eq!(fs::read(&work.log).unwrap(), bytes);
}

#[test]
fn empty_binary_and_maximum_records_survive_reopening() {
    let work = Workspace::new();
    let payloads = [vec![], vec![0, 255, 128, 10], vec![0xa5; MAX_PAYLOAD]];
    let (mut writer, report) = Writer::open(&work.log, SyncPolicy::Manual).unwrap();
    assert_eq!(report.record_count, 0);
    assert_eq!(report.valid_bytes, 16);
    let mut expected = Vec::new();
    for payload in payloads {
        let offset = writer.append(&payload).unwrap();
        expected.push((offset, payload));
    }
    writer.sync().unwrap();
    drop(writer);
    assert_eq!(read_all(&work), expected);
    let (writer, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert_eq!(report.record_count, 3);
    assert_eq!(report.discarded_bytes, 0);
    assert_eq!(report.valid_bytes, 16 + 3 * 12 + 4 + MAX_PAYLOAD as u64);
    drop(writer);
}

#[test]
fn oversized_append_leaves_a_usable_writer() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert!(matches!(
        writer.append(&vec![0; MAX_PAYLOAD + 1]),
        Err(Error::PayloadTooLarge { .. })
    ));
    assert_eq!(fs::metadata(&work.log).unwrap().len(), 16);
    assert_eq!(writer.append(b"ok").unwrap(), 16);
    drop(writer);
    assert_eq!(read_all(&work), vec![(16, b"ok".to_vec())]);
}

#[test]
fn every_final_record_cut_preserves_the_complete_prefix() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Manual).unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"second").unwrap();
    let last_offset = writer.append(b"third payload").unwrap();
    writer.sync().unwrap();
    drop(writer);
    let original = fs::read(&work.log).unwrap();

    for cut in last_offset as usize..original.len() {
        fs::write(&work.log, &original[..cut]).unwrap();
        let before = fs::read(&work.log).unwrap();
        {
            let mut reader = Reader::open(&work.log).unwrap();
            assert_eq!(reader.inspection().record_count, 2, "cut {cut}");
            assert_eq!(reader.inspection().valid_bytes, last_offset);
            let expected_tail = cut as u64 - last_offset;
            assert_eq!(
                reader.inspection().tail.map_or(0, |tail| tail.bytes),
                expected_tail
            );
            if let Some(tail) = reader.inspection().tail {
                if expected_tail < 12 {
                    assert_eq!(tail.kind, TailKind::IncompleteHeader);
                } else {
                    assert_eq!(
                        tail.kind,
                        TailKind::IncompletePayload {
                            expected: 13,
                            available: expected_tail - 12,
                        }
                    );
                }
            }
            assert_eq!(reader.next_record().unwrap().unwrap().payload, b"first");
            assert_eq!(reader.next_record().unwrap().unwrap().payload, b"second");
            assert!(reader.next_record().unwrap().is_none());
        }
        assert_eq!(
            fs::read(&work.log).unwrap(),
            before,
            "reader changed cut {cut}"
        );

        let (mut writer, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
        assert_eq!(report.record_count, 2);
        assert_eq!(report.discarded_bytes, cut as u64 - last_offset);
        assert_eq!(
            fs::read(&work.log).unwrap(),
            original[..last_offset as usize]
        );
        assert_eq!(writer.append(b"replacement").unwrap(), last_offset);
        drop(writer);
        assert_eq!(
            read_all(&work),
            vec![
                (16, b"first".to_vec()),
                (33, b"second".to_vec()),
                (last_offset, b"replacement".to_vec()),
            ]
        );
    }

    fs::write(&work.log, &original).unwrap();
    let (writer, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert_eq!(report.record_count, 3);
    assert_eq!(report.discarded_bytes, 0);
    drop(writer);
}

#[test]
fn corrupt_records_are_rejected_without_repairing_any_bytes() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Manual).unwrap();
    let first = writer.append(b"first").unwrap();
    let last = writer.append(b"last").unwrap();
    drop(writer);
    let original = fs::read(&work.log).unwrap();

    // Includes the last complete record: bad checksums aren't a recoverable tail.
    for offset in [first, last] {
        for byte in 0..16 {
            let mut damaged = original.clone();
            damaged[offset as usize + byte] ^= 0x80;
            fs::write(&work.log, &damaged).unwrap();
            assert!(matches!(
                Reader::open(&work.log),
                Err(Error::Corrupt { .. })
            ));
            assert!(matches!(
                Writer::open(&work.log, SyncPolicy::Always),
                Err(Error::Corrupt { .. })
            ));
            assert_eq!(fs::read(&work.log).unwrap(), damaged);
        }
    }

    let mut damaged = original.clone();
    damaged[first as usize + 12] ^= 1;
    damaged.extend_from_slice(&[0, 1, 2]);
    fs::write(&work.log, &damaged).unwrap();
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Always),
        Err(Error::Corrupt { .. })
    ));
    assert_eq!(fs::read(&work.log).unwrap(), damaged);
}

#[test]
fn invalid_file_headers_are_never_recreated() {
    let work = Workspace::new();
    let (writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    drop(writer);
    let original = fs::read(&work.log).unwrap();
    let mut cases: Vec<Vec<u8>> = (0..16).map(|cut| original[..cut].to_vec()).collect();
    for byte in 0..16 {
        let mut damaged = original.clone();
        damaged[byte] ^= 1;
        cases.push(damaged);
    }
    for damaged in cases {
        fs::write(&work.log, &damaged).unwrap();
        assert!(matches!(
            Reader::open(&work.log),
            Err(Error::InvalidHeader(_))
        ));
        assert!(matches!(
            Writer::open(&work.log, SyncPolicy::Always),
            Err(Error::InvalidHeader(_))
        ));
        assert_eq!(fs::read(&work.log).unwrap(), damaged);
    }
}

#[test]
fn valid_checksum_with_oversized_length_is_corruption_not_a_tail() {
    let work = Workspace::new();
    let (writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    drop(writer);
    let mut bytes = fs::read(&work.log).unwrap();
    // IEEE CRC-32 of length=1_048_577 and payload_crc=0, both little-endian.
    bytes.extend_from_slice(&[
        0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x6c, 0xdd, 0x5e, 0xaa,
    ]);
    fs::write(&work.log, &bytes).unwrap();
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Always),
        Err(Error::Corrupt {
            reason: "payload length exceeds limit",
            ..
        })
    ));
    assert_eq!(fs::read(&work.log).unwrap(), bytes);
}

#[test]
fn header_only_tail_can_be_repaired_back_to_an_empty_log() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    writer.append(b"one").unwrap();
    drop(writer);
    OpenOptions::new()
        .write(true)
        .open(&work.log)
        .unwrap()
        .set_len(20)
        .unwrap();
    let (writer, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert_eq!(report.record_count, 0);
    assert_eq!(report.valid_bytes, 16);
    assert_eq!(report.discarded_bytes, 4);
    drop(writer);
    assert!(read_all(&work).is_empty());
}

#[test]
fn locks_exclude_writers_and_allow_multiple_readers() {
    let work = Workspace::new();
    let (writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert!(matches!(Reader::open(&work.log), Err(Error::Busy)));
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Manual),
        Err(Error::Busy)
    ));
    drop(writer);
    let first = Reader::open(&work.log).unwrap();
    let second = Reader::open(&work.log).unwrap();
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Always),
        Err(Error::Busy)
    ));
    drop(first);
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Always),
        Err(Error::Busy)
    ));
    drop(second);
    assert!(Writer::open(&work.log, SyncPolicy::Always).is_ok());
}
