use std::{error::Error, io, thread};

use appendlog::{GroupOptions, GroupWriter};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("usage: group_commit <path>")?;
    if args.next().is_some() {
        return Err("usage: group_commit <path>".into());
    }
    let (writer, recovery) = GroupWriter::open(path, GroupOptions::default())?;
    let mut offsets = thread::scope(|scope| -> Result<Vec<u64>, Box<dyn Error>> {
        let mut producers = Vec::new();
        for producer in 0..4 {
            let appender = writer.appender();
            producers.push(scope.spawn(move || {
                let mut commits = Vec::new();
                for sequence in 0..8 {
                    let payload = format!("producer={producer} sequence={sequence}");
                    commits.push(appender.submit(payload.as_bytes())?);
                }
                commits
                    .into_iter()
                    .map(|commit| commit.wait())
                    .collect::<Result<Vec<_>, _>>()
            }));
        }
        let mut offsets = Vec::new();
        for producer in producers {
            let committed: Vec<u64> = producer
                .join()
                .map_err(|_| io::Error::other("producer panicked"))??;
            offsets.extend(committed);
        }
        Ok(offsets)
    })?;
    writer.shutdown()?;
    offsets.sort_unstable();
    println!(
        "appended={} first={} last={} recovered_bytes={}",
        offsets.len(),
        offsets[0],
        offsets[offsets.len() - 1],
        recovery.discarded_bytes,
    );
    Ok(())
}
