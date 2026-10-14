mod support;

use std::{
    fs,
    process::{Command, Output},
};

use appendlog::{Reader, SyncPolicy, Writer};
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
