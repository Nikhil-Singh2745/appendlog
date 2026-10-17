mod support;

use std::{
    fs,
    process::{Command, Output},
};

use appendlog::{Reader, SegmentedOptions, SegmentedReader, SegmentedWriter, SyncPolicy, Writer};
use support::Workspace;

fn cli(work: &Workspace, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_appendlog"))
        .current_dir(&work.dir)
        .args(args)
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> &str {
    std::str::from_utf8(&output.stdout).unwrap()
}

#[test]
fn commands_round_trip_and_report_the_format() {
    let work = Workspace::new();
    let init = cli(&work, &["init", "events.alog"]);
    assert!(init.status.success(), "{:?}", init);
    assert_eq!(stdout(&init), "ready records=0 valid_bytes=16\n");
    let append = cli(&work, &["append", "events.alog", "hello"]);
    assert!(append.status.success());
    assert_eq!(stdout(&append), "offset=16 bytes=5 sync=always\n");
    let append = cli(&work, &["append", "events.alog", "", "--sync", "manual"]);
    assert!(append.status.success());
    assert_eq!(stdout(&append), "offset=33 bytes=0 sync=manual\n");
    let read = cli(&work, &["read", "events.alog"]);
    assert!(read.status.success());
    assert_eq!(stdout(&read), "16\t68656c6c6f\n33\t\n");
    let inspect = cli(&work, &["inspect", "events.alog"]);
    assert!(inspect.status.success());
    assert_eq!(
        stdout(&inspect),
        "version=1\nrecords=2\nfile_bytes=45\nvalid_bytes=45\ntail=none\n"
    );
}

#[test]
fn bad_arguments_do_not_create_a_file() {
    let work = Workspace::new();
    for args in [
        vec![],
        vec!["unknown"],
        vec!["init", "events.alog", "extra"],
        vec!["append", "events.alog"],
        vec!["append", "events.alog", "x", "--sync", "later"],
        vec!["append", "events.alog", "x", "--typo", "always"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(2));
        assert!(!work.log.exists());
    }
    assert!(cli(&work, &["--help"]).status.success());
    assert_eq!(cli(&work, &["read", "missing.alog"]).status.code(), Some(1));
}

#[test]
fn inspection_is_read_only_and_append_reports_tail_recovery() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"tail").unwrap();
    drop(writer);
    let mut bytes = fs::read(&work.log).unwrap();
    bytes.truncate(bytes.len() - 2);
    fs::write(&work.log, &bytes).unwrap();
    let inspect = cli(&work, &["inspect", "events.alog"]);
    assert!(inspect.status.success());
    assert!(stdout(&inspect).contains("tail=incomplete_payload expected=4 available=2"));
    let read = cli(&work, &["read", "events.alog"]);
    assert!(read.status.success());
    assert_eq!(stdout(&read), "16\t6669727374\n");
    assert!(!read.stderr.is_empty());
    assert_eq!(fs::read(&work.log).unwrap(), bytes);

    let append = cli(&work, &["append", "events.alog", "new"]);
    assert!(append.status.success());
    assert_eq!(
        std::str::from_utf8(&append.stderr).unwrap(),
        "recovered tail: discarded 14 bytes\n"
    );
    let read = cli(&work, &["read", "events.alog"]);
    assert_eq!(stdout(&read), "16\t6669727374\n33\t6e6577\n");
}

#[test]
fn separate_processes_respect_the_locks() {
    let work = Workspace::new();
    let (writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    for args in [
        vec!["read", "events.alog"],
        vec!["append", "events.alog", "x"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(1));
        assert!(
            std::str::from_utf8(&output.stderr)
                .unwrap()
                .contains("locked")
        );
    }
    drop(writer);
    let reader = Reader::open(&work.log).unwrap();
    assert!(cli(&work, &["read", "events.alog"]).status.success());
    assert_eq!(
        cli(&work, &["append", "events.alog", "x"]).status.code(),
        Some(1)
    );
    drop(reader);
    assert!(cli(&work, &["append", "events.alog", "x"]).status.success());
}

#[test]
fn corrupt_input_returns_an_error_without_partial_stdout_or_repair() {
    let work = Workspace::new();
    let (mut writer, _) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"second").unwrap();
    drop(writer);
    let mut bytes = fs::read(&work.log).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&work.log, &bytes).unwrap();
    for args in [
        vec!["read", "events.alog"],
        vec!["inspect", "events.alog"],
        vec!["append", "events.alog", "x"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(
            std::str::from_utf8(&output.stderr)
                .unwrap()
                .contains("checksum mismatch")
        );
        assert_eq!(fs::read(&work.log).unwrap(), bytes);
    }
}

#[test]
fn directory_commands_rotate_and_print_segment_positions() {
    let work = Workspace::new();
    fs::create_dir(work.dir.join("segments")).unwrap();
    let init = cli(&work, &["init-dir", "segments"]);
    assert!(init.status.success(), "{init:?}");
    assert_eq!(stdout(&init), "ready segments=1 records=0 valid_bytes=16\n");
    let first = cli(&work, &["append-dir", "segments", "hello"]);
    assert!(first.status.success());
    assert_eq!(stdout(&first), "segment=0 offset=16 bytes=5 sync=always\n");
    let second = cli(
        &work,
        &[
            "append-dir",
            "segments",
            "world",
            "--segment-bytes",
            "50",
            "--sync",
            "always",
        ],
    );
    assert!(second.status.success());
    assert_eq!(stdout(&second), "segment=0 offset=33 bytes=5 sync=always\n");
    let third = cli(
        &work,
        &[
            "append-dir",
            "segments",
            "",
            "--sync",
            "manual",
            "--segment-bytes",
            "50",
        ],
    );
    assert!(third.status.success());
    assert_eq!(stdout(&third), "segment=1 offset=16 bytes=0 sync=manual\n");
    let read = cli(&work, &["read-dir", "segments"]);
    assert!(read.status.success());
    assert_eq!(
        stdout(&read),
        "0:16\t68656c6c6f\n0:33\t776f726c64\n1:16\t\n"
    );
    let inspect = cli(&work, &["inspect-dir", "segments"]);
    assert!(inspect.status.success());
    assert_eq!(
        stdout(&inspect),
        "segments=2\nactive_segment=1\nrecords=3\nfile_bytes=78\nvalid_bytes=78\ntail=none\n"
    );
}

#[test]
fn bad_directory_arguments_do_not_initialize_a_log() {
    let work = Workspace::new();
    let directory = work.dir.join("segments");
    fs::create_dir(&directory).unwrap();
    for args in [
        vec!["append-dir", "segments"],
        vec!["init-dir", "segments", "extra"],
        vec!["read-dir", "segments", "extra"],
        vec!["append-dir", "segments", "x", "--sync"],
        vec!["append-dir", "segments", "x", "--sync", "later"],
        vec!["append-dir", "segments", "x", "--segment-bytes", "27"],
        vec!["append-dir", "segments", "x", "--segment-bytes", "-1"],
        vec!["append-dir", "segments", "x", "--segment-bytes", "+28"],
        vec![
            "append-dir",
            "segments",
            "x",
            "--segment-bytes",
            "18446744073709551616",
        ],
        vec!["append-dir", "segments", "x", "--segment-bytes", "no"],
        vec![
            "append-dir",
            "segments",
            "x",
            "--sync",
            "always",
            "--sync",
            "manual",
        ],
        vec![
            "append-dir",
            "segments",
            "x",
            "--segment-bytes",
            "28",
            "--segment-bytes",
            "29",
        ],
        vec!["append-dir", "segments", "x", "--typo", "28"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    }
    for command in ["init-dir", "append-dir", "read-dir", "inspect-dir"] {
        let args = if command == "append-dir" {
            vec![command, "missing", "x"]
        } else {
            vec![command, "missing"]
        };
        assert_eq!(cli(&work, &args).status.code(), Some(1));
        assert!(!work.dir.join("missing").exists());
    }
}

#[test]
fn directory_tail_inspection_is_read_only_and_init_reports_repair() {
    let work = Workspace::new();
    let directory = work.dir.join("segments");
    fs::create_dir(&directory).unwrap();
    let (mut writer, _) = SegmentedWriter::open(
        &directory,
        SegmentedOptions {
            max_segment_bytes: 28,
            ..Default::default()
        },
    )
    .unwrap();
    writer.append(b"hello").unwrap();
    writer.append(b"tail").unwrap();
    drop(writer);
    let active = directory.join("00000000000000000001.alog");
    let mut bytes = fs::read(&active).unwrap();
    bytes.truncate(bytes.len() - 2);
    fs::write(&active, &bytes).unwrap();
    let inspect = cli(&work, &["inspect-dir", "segments"]);
    assert!(inspect.status.success());
    assert!(stdout(&inspect).contains("tail_segment=1\ntail_offset=16 tail_bytes=14\ntail=incomplete_payload expected=4 available=2"));
    let read = cli(&work, &["read-dir", "segments"]);
    assert!(read.status.success());
    assert_eq!(stdout(&read), "0:16\t68656c6c6f\n");
    assert!(String::from_utf8_lossy(&read.stderr).contains("1:16"));
    assert_eq!(fs::read(&active).unwrap(), bytes);
    fs::write(directory.join(".pending"), b"partial").unwrap();
    let init = cli(&work, &["init-dir", "segments"]);
    assert!(init.status.success());
    assert_eq!(stdout(&init), "ready segments=2 records=1 valid_bytes=49\n");
    assert_eq!(
        String::from_utf8_lossy(&init.stderr),
        "recovered segment 1 tail: discarded 14 bytes\nremoved unpublished staging file\n"
    );
    let append = cli(&work, &["append-dir", "segments", "new"]);
    assert!(append.status.success());
    assert_eq!(stdout(&append), "segment=1 offset=16 bytes=3 sync=always\n");
}

#[test]
fn directory_corruption_fails_before_output_or_repair() {
    let work = Workspace::new();
    let directory = work.dir.join("segments");
    fs::create_dir(&directory).unwrap();
    let (mut writer, _) = SegmentedWriter::open(
        &directory,
        SegmentedOptions {
            max_segment_bytes: 28,
            ..Default::default()
        },
    )
    .unwrap();
    writer.append(b"first").unwrap();
    writer.append(b"second").unwrap();
    drop(writer);
    let active = directory.join("00000000000000000001.alog");
    let mut bytes = fs::read(&active).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&active, &bytes).unwrap();
    for args in [
        vec!["read-dir", "segments"],
        vec!["inspect-dir", "segments"],
        vec!["init-dir", "segments"],
        vec!["append-dir", "segments", "retry"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("segment 1"));
        assert_eq!(fs::read(&active).unwrap(), bytes);
    }
}

#[test]
fn directory_locks_work_across_processes() {
    let work = Workspace::new();
    let directory = work.dir.join("segments");
    fs::create_dir(&directory).unwrap();
    let (writer, _) = SegmentedWriter::open(&directory, SegmentedOptions::default()).unwrap();
    for args in [
        vec!["read-dir", "segments"],
        vec!["inspect-dir", "segments"],
        vec!["init-dir", "segments"],
        vec!["append-dir", "segments", "x"],
    ] {
        let output = cli(&work, &args);
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("locked"));
    }
    drop(writer);
    let reader = SegmentedReader::open(&directory).unwrap();
    assert!(cli(&work, &["read-dir", "segments"]).status.success());
    assert_eq!(
        cli(&work, &["append-dir", "segments", "x"]).status.code(),
        Some(1)
    );
    drop(reader);
    assert!(
        cli(&work, &["append-dir", "segments", "x"])
            .status
            .success()
    );
}

#[cfg(unix)]
#[test]
fn directory_commands_accept_non_utf8_paths() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let work = Workspace::new();
    let directory = work.dir.join(OsString::from_vec(b"segments-\xff".to_vec()));
    fs::create_dir(&directory).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_appendlog"))
        .arg("append-dir")
        .arg(&directory)
        .arg("hello")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let output = Command::new(env!("CARGO_BIN_EXE_appendlog"))
        .arg("read-dir")
        .arg(&directory)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(stdout(&output), "0:16\t68656c6c6f\n");
}
