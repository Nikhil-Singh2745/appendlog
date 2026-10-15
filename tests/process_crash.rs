#![cfg(target_os = "linux")]

mod support;

use std::{
    io::{self, BufRead, Read, Write},
    os::unix::process::ExitStatusExt,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use appendlog::{Error, GroupOptions, GroupWriter, Reader, SyncPolicy, Writer};
use support::Workspace;

const PAYLOADS: [&[u8]; 3] = [b"first", b"", b"\0\xff\n"];
const READY: &str = "APPENDLOG_READY ";

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn always_acknowledged_records_survive_a_killed_writer() {
    acknowledged_prefix_survives("always");
}

#[test]
fn manually_synced_records_survive_a_killed_writer() {
    acknowledged_prefix_survives("manual");
}

#[test]
fn group_committed_records_survive_a_killed_writer() {
    acknowledged_prefix_survives("group");
}

fn acknowledged_prefix_survives(policy: &str) {
    let work = Workspace::new();
    let mut process = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "writer_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("APPENDLOG_TEST_PATH", &work.log)
            .env("APPENDLOG_TEST_POLICY", policy)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );

    let stdout = process.0.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let output = thread::spawn(move || {
        let result = (|| {
            for line in io::BufReader::new(stdout).lines() {
                let line = line?;
                if let Some(offsets) = line.strip_prefix(READY) {
                    return Ok(offsets.to_owned());
                }
            }
            Err(io::Error::other("writer exited before acknowledgment"))
        })();
        let _ = sender.send(result);
    });
    let offsets = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("writer acknowledgment timed out")
        .expect("read writer acknowledgment");
    output.join().unwrap();
    let offsets: Vec<u64> = offsets
        .split(',')
        .map(|value| value.parse().unwrap())
        .collect();
    assert_eq!(offsets, [16, 33, 45]);

    assert!(matches!(Reader::open(&work.log), Err(Error::Busy)));
    assert!(matches!(
        Writer::open(&work.log, SyncPolicy::Always),
        Err(Error::Busy)
    ));
    process.0.kill().unwrap();
    assert_eq!(process.0.wait().unwrap().signal(), Some(9));

    let end = offsets[2] + 12 + PAYLOADS[2].len() as u64;
    let mut reader = Reader::open(&work.log).unwrap();
    assert_eq!(reader.inspection().record_count, 3);
    assert_eq!(reader.inspection().file_bytes, end);
    assert_eq!(reader.inspection().valid_bytes, end);
    assert_eq!(reader.inspection().tail, None);
    for (&offset, &payload) in offsets.iter().zip(&PAYLOADS) {
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.offset, offset);
        assert_eq!(record.payload, payload);
    }
    assert!(reader.next_record().unwrap().is_none());
    drop(reader);

    let (mut writer, recovery) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
    assert_eq!(recovery.record_count, 3);
    assert_eq!(recovery.valid_bytes, end);
    assert_eq!(recovery.discarded_bytes, 0);
    assert_eq!(writer.append(b"after restart").unwrap(), end);
    drop(writer);

    let mut reader = Reader::open(&work.log).unwrap();
    for (&offset, &payload) in offsets.iter().zip(&PAYLOADS) {
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(
            (record.offset, record.payload.as_slice()),
            (offset, payload)
        );
    }
    let record = reader.next_record().unwrap().unwrap();
    assert_eq!(record.offset, end);
    assert_eq!(record.payload, b"after restart");
    assert!(reader.next_record().unwrap().is_none());
}

#[test]
#[ignore = "subprocess helper, started by the crash tests"]
fn writer_process() {
    let path = std::env::var_os("APPENDLOG_TEST_PATH").expect("child log path");
    let policy = std::env::var("APPENDLOG_TEST_POLICY").unwrap();
    if policy == "group" {
        let (writer, _) = GroupWriter::open(path, GroupOptions::default()).unwrap();
        let appender = writer.appender();
        let commits: Vec<_> = PAYLOADS
            .iter()
            .map(|payload| appender.submit(payload).unwrap())
            .collect();
        let offsets: Vec<_> = commits
            .into_iter()
            .map(|commit| commit.wait().unwrap().to_string())
            .collect();
        wait_for_kill(&offsets);
    }
    let policy = match policy.as_str() {
        "always" => SyncPolicy::Always,
        "manual" => SyncPolicy::Manual,
        other => panic!("unknown sync policy: {other}"),
    };
    let (mut writer, _) = Writer::open(path, policy).unwrap();
    let offsets: Vec<_> = PAYLOADS
        .iter()
        .map(|payload| writer.append(payload).unwrap().to_string())
        .collect();
    if policy == SyncPolicy::Manual {
        writer.sync().unwrap();
    }
    wait_for_kill(&offsets);
}

fn wait_for_kill(offsets: &[String]) -> ! {
    // Start a fresh line after libtest's output, then acknowledge the barrier.
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "\n{READY}{}", offsets.join(",")).unwrap();
    stdout.flush().unwrap();
    drop(stdout);

    io::stdin()
        .read_exact(&mut [0])
        .expect("parent keeps stdin open");
    panic!("parent should kill the writer while it holds the lock");
}
