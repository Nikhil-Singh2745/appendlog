use std::{
    env,
    ffi::OsString,
    io::{self, BufWriter, Write},
    path::PathBuf,
    process::ExitCode,
};

use appendlog::{
    Reader, RecoveryReport, SegmentedOptions, SegmentedReader, SegmentedRecoveryReport,
    SegmentedWriter, SyncPolicy, Tail, TailKind, Writer,
};

const USAGE: &str = "usage:
  appendlog init <path>
  appendlog append <path> <text> [--sync always|manual]
  appendlog read <path>
  appendlog inspect <path>
  appendlog init-dir <directory>
  appendlog append-dir <directory> <text> [--sync always|manual] [--segment-bytes N]
  appendlog read-dir <directory>
  appendlog inspect-dir <directory>";

enum Command {
    Help,
    Init(PathBuf),
    Append {
        path: PathBuf,
        text: String,
        policy: SyncPolicy,
    },
    Read(PathBuf),
    Inspect(PathBuf),
    InitDir(PathBuf),
    AppendDir {
        path: PathBuf,
        text: String,
        options: SegmentedOptions,
    },
    ReadDir(PathBuf),
    InspectDir(PathBuf),
}

fn parse(args: Vec<OsString>) -> Result<Command, &'static str> {
    let Some(command) = args.first().and_then(|arg| arg.to_str()) else {
        return Err("missing command");
    };
    if matches!(command, "--help" | "-h") && args.len() == 1 {
        return Ok(Command::Help);
    }
    match (command, args.len()) {
        ("init", 2) => Ok(Command::Init(args[1].clone().into())),
        ("read", 2) => Ok(Command::Read(args[1].clone().into())),
        ("inspect", 2) => Ok(Command::Inspect(args[1].clone().into())),
        ("init-dir", 2) => Ok(Command::InitDir(args[1].clone().into())),
        ("read-dir", 2) => Ok(Command::ReadDir(args[1].clone().into())),
        ("inspect-dir", 2) => Ok(Command::InspectDir(args[1].clone().into())),
        ("append-dir", 3 | 5 | 7) => {
            let text = args[2]
                .to_str()
                .ok_or("text must be valid UTF-8")?
                .to_owned();
            let mut options = SegmentedOptions::default();
            let (mut sync_seen, mut size_seen) = (false, false);
            for pair in args[3..].chunks_exact(2) {
                match pair[0].to_str() {
                    Some("--sync") if !sync_seen => {
                        sync_seen = true;
                        options.sync_policy = match pair[1].to_str() {
                            Some("always") => SyncPolicy::Always,
                            Some("manual") => SyncPolicy::Manual,
                            _ => return Err("sync policy must be always or manual"),
                        };
                    }
                    Some("--segment-bytes") if !size_seen => {
                        size_seen = true;
                        let value = pair[1]
                            .to_str()
                            .ok_or("segment limit must be an integer of at least 28")?;
                        if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                            return Err("segment limit must be an integer of at least 28");
                        }
                        options.max_segment_bytes = value
                            .parse()
                            .map_err(|_| "segment limit must be an integer of at least 28")?;
                        if options.max_segment_bytes < 28 {
                            return Err("segment limit must be at least 28 bytes");
                        }
                    }
                    _ => return Err("unknown or repeated append-dir option"),
                }
            }
            Ok(Command::AppendDir {
                path: args[1].clone().into(),
                text,
                options,
            })
        }
        ("append", 3 | 5) => {
            let text = args[2]
                .to_str()
                .ok_or("text must be valid UTF-8")?
                .to_owned();
            let policy = if args.len() == 3 {
                SyncPolicy::Always
            } else {
                if args[3] != "--sync" {
                    return Err("expected --sync always|manual");
                }
                match args[4].to_str() {
                    Some("always") => SyncPolicy::Always,
                    Some("manual") => SyncPolicy::Manual,
                    _ => return Err("sync policy must be always or manual"),
                }
            };
            Ok(Command::Append {
                path: args[1].clone().into(),
                text,
                policy,
            })
        }
        _ => Err("unknown command or wrong number of arguments"),
    }
}

fn report_recovery(report: &RecoveryReport) {
    if report.discarded_bytes != 0 {
        eprintln!("recovered tail: discarded {} bytes", report.discarded_bytes);
    }
}

fn report_segmented_recovery(report: &SegmentedRecoveryReport) {
    if report.discarded_bytes != 0 {
        eprintln!(
            "recovered segment {} tail: discarded {} bytes",
            report.active_segment, report.discarded_bytes
        );
    }
    if report.removed_staging {
        eprintln!("removed unpublished staging file");
    }
}

fn print_tail(output: &mut impl Write, tail: Option<Tail>) -> io::Result<()> {
    match tail {
        None => writeln!(output, "tail=none"),
        Some(tail) => {
            writeln!(
                output,
                "tail_offset={} tail_bytes={}",
                tail.offset, tail.bytes
            )?;
            match tail.kind {
                TailKind::IncompleteHeader => writeln!(output, "tail=incomplete_header"),
                TailKind::IncompletePayload {
                    expected,
                    available,
                } => writeln!(
                    output,
                    "tail=incomplete_payload expected={expected} available={available}"
                ),
            }
        }
    }
}

fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = BufWriter::new(io::stdout().lock());
    match command {
        Command::Help => writeln!(output, "{USAGE}")?,
        Command::Init(path) => {
            let (_writer, report) = Writer::open(path, SyncPolicy::Always)?;
            report_recovery(&report);
            writeln!(
                output,
                "ready records={} valid_bytes={}",
                report.record_count, report.valid_bytes
            )?;
        }
        Command::Append { path, text, policy } => {
            let (mut writer, report) = Writer::open(path, policy)?;
            report_recovery(&report);
            let offset = writer.append(text.as_bytes())?;
            let sync = match policy {
                SyncPolicy::Always => "always",
                SyncPolicy::Manual => "manual",
            };
            writeln!(output, "offset={offset} bytes={} sync={sync}", text.len())?;
        }
        Command::Read(path) => {
            let mut reader = Reader::open(path)?;
            if let Some(tail) = reader.inspection().tail {
                eprintln!(
                    "incomplete tail at byte {}: {} bytes (left unchanged)",
                    tail.offset, tail.bytes
                );
            }
            while let Some(record) = reader.next_record()? {
                write!(output, "{}\t", record.offset)?;
                for byte in record.payload {
                    write!(output, "{byte:02x}")?;
                }
                writeln!(output)?;
            }
        }
        Command::Inspect(path) => {
            let reader = Reader::open(path)?;
            let info = reader.inspection();
            writeln!(output, "version={}", info.version)?;
            writeln!(output, "records={}", info.record_count)?;
            writeln!(output, "file_bytes={}", info.file_bytes)?;
            writeln!(output, "valid_bytes={}", info.valid_bytes)?;
            print_tail(&mut output, info.tail)?;
        }
        Command::InitDir(path) => {
            let (_writer, report) = SegmentedWriter::open(path, SegmentedOptions::default())?;
            report_segmented_recovery(&report);
            writeln!(
                output,
                "ready segments={} records={} valid_bytes={}",
                report.active_segment + 1,
                report.record_count,
                report.valid_bytes
            )?;
        }
        Command::AppendDir {
            path,
            text,
            options,
        } => {
            let (mut writer, report) = SegmentedWriter::open(path, options)?;
            report_segmented_recovery(&report);
            let position = writer.append(text.as_bytes())?;
            let sync = match options.sync_policy {
                SyncPolicy::Always => "always",
                SyncPolicy::Manual => "manual",
            };
            writeln!(
                output,
                "segment={} offset={} bytes={} sync={sync}",
                position.segment_id,
                position.offset,
                text.len()
            )?;
        }
        Command::ReadDir(path) => {
            let mut reader = SegmentedReader::open(path)?;
            let active = reader.inspection().last().expect("reader has a segment");
            if let Some(tail) = active.inspection.tail {
                eprintln!(
                    "incomplete tail at {}:{}: {} bytes (left unchanged)",
                    active.segment_id, tail.offset, tail.bytes
                );
            }
            while let Some(record) = reader.next_record()? {
                write!(
                    output,
                    "{}:{}\t",
                    record.position.segment_id, record.position.offset
                )?;
                for byte in record.payload {
                    write!(output, "{byte:02x}")?;
                }
                writeln!(output)?;
            }
        }
        Command::InspectDir(path) => {
            let reader = SegmentedReader::open(path)?;
            let segments = reader.inspection();
            let active = segments.last().expect("reader has a segment");
            writeln!(output, "segments={}", segments.len())?;
            writeln!(output, "active_segment={}", active.segment_id)?;
            for (name, value) in [
                (
                    "records",
                    segments
                        .iter()
                        .map(|s| s.inspection.record_count)
                        .sum::<u64>(),
                ),
                (
                    "file_bytes",
                    segments.iter().map(|s| s.inspection.file_bytes).sum(),
                ),
                (
                    "valid_bytes",
                    segments.iter().map(|s| s.inspection.valid_bytes).sum(),
                ),
            ] {
                writeln!(output, "{name}={value}")?;
            }
            if active.inspection.tail.is_some() {
                writeln!(output, "tail_segment={}", active.segment_id)?;
            }
            print_tail(&mut output, active.inspection.tail)?;
        }
    }
    output.flush()?;
    Ok(())
}

fn main() -> ExitCode {
    let command = match parse(env::args_os().skip(1).collect()) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("appendlog: {error}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("appendlog: {error}");
            ExitCode::FAILURE
        }
    }
}
