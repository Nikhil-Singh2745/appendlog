use std::{
    env,
    ffi::OsString,
    io::{self, BufWriter, Write},
    path::PathBuf,
    process::ExitCode,
};

use appendlog::{Reader, RecoveryReport, SyncPolicy, TailKind, Writer};

const USAGE: &str = "usage:
  appendlog init <path>
  appendlog append <path> <text> [--sync always|manual]
  appendlog read <path>
  appendlog inspect <path>";

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
            match info.tail {
                None => writeln!(output, "tail=none")?,
                Some(tail) => {
                    writeln!(
                        output,
                        "tail_offset={} tail_bytes={}",
                        tail.offset, tail.bytes
                    )?;
                    match tail.kind {
                        TailKind::IncompleteHeader => writeln!(output, "tail=incomplete_header")?,
                        TailKind::IncompletePayload {
                            expected,
                            available,
                        } => {
                            writeln!(
                                output,
                                "tail=incomplete_payload expected={expected} available={available}"
                            )?;
                        }
                    }
                }
            }
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
