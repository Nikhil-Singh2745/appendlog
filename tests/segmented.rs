mod support;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use appendlog::{
    Error, LogPosition, MAX_PAYLOAD, SegmentedOptions, SegmentedReader, SegmentedWriter,
    SyncPolicy, Writer,
};
use support::Workspace;

fn workspace() -> Workspace {
    let work = Workspace::new();
    fs::create_dir(&work.log).unwrap();
    work
}

fn options(bytes: u64) -> SegmentedOptions {
    SegmentedOptions {
        max_segment_bytes: bytes,
        ..Default::default()
    }
}

fn segment(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("{id:020}.alog"))
}

fn records(directory: &Path) -> Vec<(LogPosition, Vec<u8>)> {
    let mut reader = SegmentedReader::open(directory).unwrap();
    let mut result = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        result.push((record.position, record.payload));
    }
    assert!(reader.next_record().unwrap().is_none());
    result
}

fn snapshot(directory: &Path) -> BTreeMap<OsString, Vec<u8>> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect()
}

#[test]
fn initializes_an_empty_log_and_reopens_without_changes() {
    let work = workspace();
    assert!(SegmentedReader::open(&work.log).is_err());
    assert_eq!(fs::read_dir(&work.log).unwrap().count(), 0);
    let (writer, report) = SegmentedWriter::open(&work.log, options(28)).unwrap();
    assert_eq!(report.record_count, 0);
    assert_eq!(report.valid_bytes, 16);
    assert_eq!(report.active_segment, 0);
    assert_eq!(report.discarded_bytes, 0);
    assert!(!report.removed_staging);
    drop(writer);
    let before = snapshot(&work.log);
    assert!(records(&work.log).is_empty());
    let (writer, report) = SegmentedWriter::open(&work.log, options(28)).unwrap();
    assert_eq!(report.valid_bytes, 16);
    drop(writer);
    assert_eq!(snapshot(&work.log), before);
}

#[test]
fn exact_fit_rotation_and_policy_changes_preserve_positions() {
    let work = workspace();
    let (mut writer, _) = SegmentedWriter::open(&work.log, options(50)).unwrap();
    let payloads: &[&[u8]] = &[b"hello", b"world", b"", &[0, 255, 128]];
    let expected_positions = [(0, 16), (0, 33), (1, 16), (1, 28)];
    let mut expected = Vec::new();
    for (&payload, (segment_id, offset)) in payloads.iter().zip(expected_positions) {
        let position = writer.append(payload).unwrap();
        assert_eq!(position, LogPosition { segment_id, offset });
        expected.push((position, payload.to_vec()));
    }
    drop(writer);
    assert_eq!(fs::metadata(segment(&work.log, 0)).unwrap().len(), 50);
    assert_eq!(fs::metadata(segment(&work.log, 1)).unwrap().len(), 43);
    assert_eq!(records(&work.log), expected);

    let (mut writer, report) = SegmentedWriter::open(&work.log, options(28)).unwrap();
    assert_eq!(report.record_count, 4);
    assert_eq!(report.valid_bytes, 93);
    let position = writer.append(b"x").unwrap();
    assert_eq!(
        position,
        LogPosition {
            segment_id: 2,
            offset: 16
        }
    );
    expected.push((position, b"x".to_vec()));
    drop(writer);
    let (mut writer, _) = SegmentedWriter::open(&work.log, options(100)).unwrap();
    let position = writer.append(b"y").unwrap();
    assert_eq!(
        position,
        LogPosition {
            segment_id: 2,
            offset: 29
        }
    );
    expected.push((position, b"y".to_vec()));
    drop(writer);
    assert_eq!(records(&work.log), expected);
}

#[test]
fn repeated_rotation_and_large_frames_do_not_create_empty_segments() {
    let work = workspace();
    let mut config = options(28);
    config.sync_policy = SyncPolicy::Manual;
    let (mut writer, _) = SegmentedWriter::open(&work.log, config).unwrap();
    let mut expected = Vec::new();
    for id in 0..32 {
        let payload = vec![id as u8];
        let position = writer.append(&payload).unwrap();
        assert_eq!(
            position,
            LogPosition {
                segment_id: id,
                offset: 16
            }
        );
        expected.push((position, payload));
    }
    let payload = vec![0xa5; MAX_PAYLOAD];
    let position = writer.append(&payload).unwrap();
    assert_eq!(
        position,
        LogPosition {
            segment_id: 32,
            offset: 16
        }
    );
    expected.push((position, payload));
    assert!(matches!(
        writer.append(&vec![0; MAX_PAYLOAD + 1]),
        Err(Error::PayloadTooLarge { .. })
    ));
    assert!(!segment(&work.log, 33).exists());
    let position = writer.append(b"").unwrap();
    assert_eq!(
        position,
        LogPosition {
            segment_id: 33,
            offset: 16
        }
    );
    expected.push((position, Vec::new()));
    writer.sync().unwrap();
    drop(writer);
    assert_eq!(records(&work.log), expected);
    assert_eq!(
        SegmentedReader::open(&work.log).unwrap().inspection().len(),
        34
    );
}

#[test]
fn every_active_record_cut_is_read_only_until_writer_recovery() {
    let work = workspace();
    drop(SegmentedWriter::open(&work.log, options(100)).unwrap());
    let (mut first, _) = Writer::open(segment(&work.log, 0), SyncPolicy::Always).unwrap();
    first.append(b"sealed").unwrap();
    drop(first);
    let (mut active, _) = Writer::open(segment(&work.log, 1), SyncPolicy::Always).unwrap();
    active.append(b"kept").unwrap();
    let boundary = active.append(b"final payload").unwrap();
    drop(active);
    let original = fs::read(segment(&work.log, 1)).unwrap();
    let sealed = fs::read(segment(&work.log, 0)).unwrap();
    for cut in boundary as usize..original.len() {
        fs::write(segment(&work.log, 1), &original[..cut]).unwrap();
        let before = snapshot(&work.log);
        {
            let reader = SegmentedReader::open(&work.log).unwrap();
            let active = reader.inspection().last().unwrap();
            assert_eq!(active.inspection.valid_bytes, boundary);
            assert_eq!(
                active.inspection.tail.map_or(0, |tail| tail.bytes),
                cut as u64 - boundary
            );
        }
        assert_eq!(records(&work.log).len(), 2);
        assert_eq!(snapshot(&work.log), before, "cut {cut}");
        let (mut writer, report) = SegmentedWriter::open(&work.log, options(100)).unwrap();
        assert_eq!(report.record_count, 2);
        assert_eq!(report.discarded_bytes, cut as u64 - boundary);
        assert_eq!(report.valid_bytes, sealed.len() as u64 + boundary);
        assert_eq!(
            writer.append(b"replacement").unwrap(),
            LogPosition {
                segment_id: 1,
                offset: boundary
            }
        );
        drop(writer);
        assert_eq!(fs::read(segment(&work.log, 0)).unwrap(), sealed);
        assert_eq!(records(&work.log).last().unwrap().1, b"replacement");
    }
}

#[test]
fn invalid_published_bytes_prevent_all_repairs_and_staging_cleanup() {
    for case in 0..5 {
        let work = workspace();
        let (mut writer, _) = SegmentedWriter::open(&work.log, options(28)).unwrap();
        writer.append(b"first").unwrap();
        writer.append(b"second").unwrap();
        drop(writer);
        let id = if case < 2 { 0 } else { 1 };
        let path = segment(&work.log, id);
        let mut bytes = fs::read(&path).unwrap();
        match case {
            0 | 2 => *bytes.last_mut().unwrap() ^= 0x80,
            1 => {
                bytes.pop();
            }
            3 => bytes.truncate(7),
            4 => bytes[0] ^= 1,
            _ => unreachable!(),
        }
        fs::write(&path, bytes).unwrap();
        if case < 2 {
            let path = segment(&work.log, 1);
            let mut bytes = fs::read(&path).unwrap();
            bytes.pop();
            fs::write(path, bytes).unwrap();
        }
        fs::write(work.log.join(".pending"), b"unpublished").unwrap();
        let before = snapshot(&work.log);
        for write in [false, true] {
            let error = if write {
                SegmentedWriter::open(&work.log, options(28)).err().unwrap()
            } else {
                SegmentedReader::open(&work.log).err().unwrap()
            };
            assert!(matches!(error, Error::Segment { segment_id, .. } if segment_id == id));
            assert_eq!(snapshot(&work.log), before, "case {case}");
        }
    }
}

#[test]
fn staging_is_ignored_by_readers_and_removed_by_writers() {
    for initialized in [false, true] {
        let work = workspace();
        if initialized {
            drop(SegmentedWriter::open(&work.log, options(28)).unwrap());
        }
        fs::write(work.log.join(".pending"), b"partial header").unwrap();
        if initialized {
            let before = snapshot(&work.log);
            assert!(records(&work.log).is_empty());
            assert_eq!(snapshot(&work.log), before);
        }
        let (mut writer, report) = SegmentedWriter::open(&work.log, options(28)).unwrap();
        assert!(report.removed_staging);
        assert_eq!(report.active_segment, 0);
        assert_eq!(report.valid_bytes, 16);
        assert!(!work.log.join(".pending").exists());
        assert_eq!(
            writer.append(b"ok").unwrap(),
            LogPosition {
                segment_id: 0,
                offset: 16
            }
        );
    }
}

#[test]
fn gaps_and_unexpected_names_are_rejected_without_cleanup() {
    for name in [
        "00000000000000000002.alog",
        "1.alog",
        "18446744073709551616.alog",
        "other.txt",
    ] {
        let work = workspace();
        drop(SegmentedWriter::open(&work.log, options(28)).unwrap());
        fs::write(work.log.join(name), b"unexpected").unwrap();
        fs::write(work.log.join(".pending"), b"keep").unwrap();
        let before = snapshot(&work.log);
        assert!(matches!(
            SegmentedWriter::open(&work.log, options(28)),
            Err(Error::InvalidLayout(_))
        ));
        assert!(matches!(
            SegmentedReader::open(&work.log),
            Err(Error::InvalidLayout(_))
        ));
        assert_eq!(snapshot(&work.log), before);
    }
}

#[test]
fn invalid_options_and_missing_directories_do_not_initialize_anything() {
    let work = workspace();
    for bytes in [0, 16, 27] {
        assert!(
            matches!(SegmentedWriter::open(&work.log, options(bytes)), Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput)
        );
        assert_eq!(fs::read_dir(&work.log).unwrap().count(), 0);
    }
    let missing = work.dir.join("missing");
    assert!(SegmentedWriter::open(&missing, options(28)).is_err());
    assert!(!missing.exists());
}

#[test]
fn directory_locks_exclude_writers_and_allow_shared_readers() {
    let work = workspace();
    let (writer, _) = SegmentedWriter::open(&work.log, options(28)).unwrap();
    assert!(matches!(SegmentedReader::open(&work.log), Err(Error::Busy)));
    assert!(matches!(
        SegmentedWriter::open(&work.log, options(28)),
        Err(Error::Busy)
    ));
    drop(writer);
    let reader = SegmentedReader::open(&work.log).unwrap();
    let second = SegmentedReader::open(&work.log).unwrap();
    assert!(matches!(
        SegmentedWriter::open(&work.log, options(28)),
        Err(Error::Busy)
    ));
    drop((reader, second));
    assert!(SegmentedWriter::open(&work.log, options(28)).is_ok());
}

#[cfg(unix)]
#[test]
fn reserved_entries_cannot_be_symlinks_or_directories() {
    use std::os::unix::fs::symlink;
    for name in ["LOCK", ".pending", "00000000000000000000.alog"] {
        for link in [false, true] {
            let work = workspace();
            let path = work.log.join(name);
            if link {
                fs::write(work.dir.join("outside"), b"leave alone").unwrap();
                symlink(work.dir.join("outside"), &path).unwrap();
            } else {
                fs::create_dir(&path).unwrap();
            }
            assert!(matches!(
                SegmentedWriter::open(&work.log, options(28)),
                Err(Error::InvalidLayout(_))
            ));
            assert!(path.symlink_metadata().is_ok());
            if link {
                assert_eq!(fs::read(work.dir.join("outside")).unwrap(), b"leave alone");
            }
        }
    }
}
