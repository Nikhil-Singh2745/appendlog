use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{Error, Inspection, MAX_PAYLOAD, SyncPolicy, Writer, format, storage};
use format::{FILE_HEADER_LEN, RECORD_HEADER_LEN, ReadOutcome};

const STAGING: &str = ".pending";
const LOCK: &str = "LOCK";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogPosition {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentedRecord {
    pub position: LogPosition,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct SegmentedOptions {
    pub max_segment_bytes: u64,
    pub sync_policy: SyncPolicy,
}

impl Default for SegmentedOptions {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::Always,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInspection {
    pub segment_id: u64,
    pub inspection: Inspection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentedRecoveryReport {
    pub record_count: u64,
    pub valid_bytes: u64,
    pub discarded_bytes: u64,
    pub active_segment: u64,
    pub removed_staging: bool,
}

pub struct SegmentedWriter {
    directory: PathBuf,
    directory_file: File,
    _lock: File,
    writer: Writer,
    segment_id: u64,
    options: SegmentedOptions,
    poisoned: bool,
}

impl SegmentedWriter {
    /// The directory must already exist. Segment size is a per-open policy.
    pub fn open(
        directory: impl AsRef<Path>,
        options: SegmentedOptions,
    ) -> Result<(Self, SegmentedRecoveryReport), Error> {
        Self::open_observed(directory.as_ref(), options, &mut |_, _| Ok(()))
    }

    fn open_observed(
        directory: &Path,
        options: SegmentedOptions,
        observer: &mut impl FnMut(RotationStep, bool) -> Result<(), Error>,
    ) -> Result<(Self, SegmentedRecoveryReport), Error> {
        if options.max_segment_bytes < FILE_HEADER_LEN + RECORD_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment limit must be at least 28 bytes",
            )
            .into());
        }
        let directory = &fs::canonicalize(directory)?;
        let directory_file = open_directory(directory)?;
        let lock = lock_directory(directory, true)?;
        let (ids, staging) = discover(directory)?;
        let inspections = inspect_segments(directory, &ids)?;
        let record_count = total(&inspections, |info| info.record_count)?;
        let valid_bytes = total(&inspections, |info| info.valid_bytes)?;

        // A bad published segment must not cause cleanup or tail repair elsewhere.
        if staging {
            fs::remove_file(directory.join(STAGING))?;
        }
        let segment_id = ids.last().copied().unwrap_or(0);
        let (file, valid_bytes, discarded_bytes) = if ids.is_empty() {
            let file = in_segment(0, || publish(directory, &directory_file, 0, observer))?;
            (file, FILE_HEADER_LEN, 0)
        } else {
            let mut active = None;
            let mut discarded = 0;
            for segment in &inspections {
                let id = segment.segment_id;
                let file = in_segment(id, || {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(id == segment_id)
                        .open(directory.join(segment_name(id)))?;
                    if id == segment_id {
                        discarded = segment.inspection.file_bytes - segment.inspection.valid_bytes;
                        if discarded != 0 {
                            file.set_len(segment.inspection.valid_bytes)?;
                        }
                    }
                    file.sync_all()?;
                    Ok(file)
                })?;
                if id == segment_id {
                    active = Some(file);
                }
            }
            directory_file.sync_all()?;
            (
                active.expect("nonempty segment list"),
                valid_bytes,
                discarded,
            )
        };
        let active_bytes = inspections
            .last()
            .map_or(FILE_HEADER_LEN, |s| s.inspection.valid_bytes);
        let writer = in_segment(segment_id, || {
            Writer::from_validated(file, options.sync_policy, active_bytes)
        })?;
        Ok((
            Self {
                directory: directory.to_owned(),
                directory_file,
                _lock: lock,
                writer,
                segment_id,
                options,
                poisoned: false,
            },
            SegmentedRecoveryReport {
                record_count,
                valid_bytes,
                discarded_bytes,
                active_segment: segment_id,
                removed_staging: staging,
            },
        ))
    }

    /// Returns a segment ID and local byte offset; durability follows the sync policy.
    pub fn append(&mut self, payload: &[u8]) -> Result<LogPosition, Error> {
        self.append_observed(payload, &mut |_, _| Ok(()))
    }

    fn append_observed(
        &mut self,
        payload: &[u8],
        observer: &mut impl FnMut(RotationStep, bool) -> Result<(), Error>,
    ) -> Result<LogPosition, Error> {
        self.check_ready()?;
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::PayloadTooLarge { len: payload.len() });
        }
        let end = self
            .writer
            .next_offset()
            .checked_add(RECORD_HEADER_LEN + payload.len() as u64)
            .ok_or_else(|| io::Error::other("log offset overflow"))?;
        let rotate =
            self.writer.next_offset() > FILE_HEADER_LEN && end > self.options.max_segment_bytes;
        let next_id = if rotate {
            self.segment_id
                .checked_add(1)
                .ok_or_else(|| Error::InvalidLayout("segment ID overflow".into()))?
        } else {
            self.segment_id
        };
        let result = (|| {
            if rotate {
                in_segment(self.segment_id, || {
                    step(observer, RotationStep::SyncOld, || self.writer.sync())
                })?;
                let file = in_segment(next_id, || {
                    publish(&self.directory, &self.directory_file, next_id, observer)
                })?;
                self.writer = in_segment(next_id, || {
                    Writer::from_validated(file, self.options.sync_policy, FILE_HEADER_LEN)
                })?;
                self.segment_id = next_id;
            }
            let offset = in_segment(self.segment_id, || self.writer.append(payload))?;
            Ok(LogPosition {
                segment_id: self.segment_id,
                offset,
            })
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub fn sync(&mut self) -> Result<(), Error> {
        self.check_ready()?;
        let result = in_segment(self.segment_id, || self.writer.sync());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn check_ready(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

pub struct SegmentedReader {
    directory: PathBuf,
    _lock: File,
    inspections: Vec<SegmentInspection>,
    index: usize,
    file: Option<File>,
    offset: u64,
    stopped: bool,
}

impl SegmentedReader {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, Error> {
        let directory = &fs::canonicalize(directory)?;
        open_directory(directory)?;
        let lock = lock_directory(directory, false)?;
        let (ids, _) = discover(directory)?;
        if ids.is_empty() {
            return Err(Error::InvalidLayout("no published segments".into()));
        }
        let inspections = inspect_segments(directory, &ids)?;
        Ok(Self {
            directory: directory.to_owned(),
            _lock: lock,
            inspections,
            index: 0,
            file: None,
            offset: FILE_HEADER_LEN,
            stopped: false,
        })
    }

    pub fn inspection(&self) -> &[SegmentInspection] {
        &self.inspections
    }

    pub fn next_record(&mut self) -> Result<Option<SegmentedRecord>, Error> {
        if self.stopped {
            return Ok(None);
        }
        let result = self.read_next();
        if !matches!(result, Ok(Some(_))) {
            self.stopped = true;
        }
        result
    }

    fn read_next(&mut self) -> Result<Option<SegmentedRecord>, Error> {
        while let Some(segment) = self.inspections.get(self.index).copied() {
            let id = segment.segment_id;
            if self.file.is_none() {
                let file = in_segment(id, || {
                    let mut file = File::open(self.directory.join(segment_name(id)))?;
                    file.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
                    Ok(file)
                })?;
                self.file = Some(file);
            }
            let outcome = in_segment(id, || {
                format::read_record(
                    self.file.as_mut().expect("opened current segment"),
                    self.offset,
                    segment.inspection.valid_bytes,
                )
            })?;
            match outcome {
                ReadOutcome::Record(record) => {
                    self.offset += RECORD_HEADER_LEN + record.payload.len() as u64;
                    return Ok(Some(SegmentedRecord {
                        position: LogPosition {
                            segment_id: id,
                            offset: record.offset,
                        },
                        payload: record.payload,
                    }));
                }
                ReadOutcome::End => {
                    self.file = None;
                    self.index += 1;
                    self.offset = FILE_HEADER_LEN;
                }
                ReadOutcome::Tail(_) => {
                    return in_segment(id, || {
                        Err(Error::Corrupt {
                            offset: self.offset,
                            reason: "previously validated prefix changed",
                        })
                    });
                }
            }
        }
        Ok(None)
    }
}

fn segment_name(id: u64) -> String {
    format!("{id:020}.alog")
}

fn open_directory(path: &Path) -> Result<File, Error> {
    let file = File::open(path)?;
    if !file.metadata()?.is_dir() {
        return Err(Error::InvalidLayout("path is not a directory".into()));
    }
    Ok(file)
}

fn lock_directory(directory: &Path, write: bool) -> Result<File, Error> {
    let path = directory.join(LOCK);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(Error::InvalidLayout("LOCK is not a regular file".into()));
        }
        Ok(_) => (),
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .create(write)
        .truncate(false)
        .open(path)?;
    storage::lock(if write {
        file.try_lock()
    } else {
        file.try_lock_shared()
    })?;
    Ok(file)
}

fn discover(directory: &Path) -> Result<(Vec<u64>, bool), Error> {
    let mut ids = Vec::new();
    let mut staging = false;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        if !entry.file_type()?.is_file() {
            return Err(Error::InvalidLayout(format!(
                "nonregular entry: {}",
                name.to_string_lossy()
            )));
        }
        let Some(name) = name.to_str() else {
            return Err(Error::InvalidLayout("non-UTF-8 entry name".into()));
        };
        match name {
            LOCK => (),
            STAGING => staging = true,
            _ => {
                let id = name
                    .strip_suffix(".alog")
                    .filter(|n| n.len() == 20 && n.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|n| n.parse::<u64>().ok())
                    .ok_or_else(|| Error::InvalidLayout(format!("unexpected entry: {name}")))?;
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    for (expected, &id) in ids.iter().enumerate() {
        if id != expected as u64 {
            return Err(Error::InvalidLayout(format!(
                "expected segment {expected}, found {id}"
            )));
        }
    }
    Ok((ids, staging))
}

fn inspect_segments(directory: &Path, ids: &[u64]) -> Result<Vec<SegmentInspection>, Error> {
    ids.iter()
        .map(|&segment_id| {
            in_segment(segment_id, || {
                let inspection =
                    storage::scan(&mut File::open(directory.join(segment_name(segment_id)))?)?;
                if Some(&segment_id) != ids.last()
                    && let Some(tail) = inspection.tail
                {
                    return Err(Error::Corrupt {
                        offset: tail.offset,
                        reason: "incomplete record in sealed segment",
                    });
                }
                Ok(SegmentInspection {
                    segment_id,
                    inspection,
                })
            })
        })
        .collect()
}

fn total(segments: &[SegmentInspection], field: impl Fn(&Inspection) -> u64) -> Result<u64, Error> {
    segments.iter().try_fold(0_u64, |sum, s| {
        sum.checked_add(field(&s.inspection))
            .ok_or_else(|| Error::InvalidLayout("aggregate size overflow".into()))
    })
}

fn in_segment<T>(segment_id: u64, action: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
    action().map_err(|source| Error::Segment {
        segment_id,
        source: Box::new(source),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RotationStep {
    SyncOld,
    Create,
    WriteHeader,
    SyncNew,
    Rename,
    SyncDirectory,
}

// Private observation points exercise the real publication path, including process death.
fn step<T>(
    observer: &mut impl FnMut(RotationStep, bool) -> Result<(), Error>,
    point: RotationStep,
    action: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    observer(point, false)?;
    let result = action()?;
    observer(point, true)?;
    Ok(result)
}

fn publish(
    directory: &Path,
    directory_file: &File,
    id: u64,
    observer: &mut impl FnMut(RotationStep, bool) -> Result<(), Error>,
) -> Result<File, Error> {
    let pending = directory.join(STAGING);
    let destination = directory.join(segment_name(id));
    if destination.try_exists()? {
        return Err(Error::InvalidLayout(
            "publication target already exists".into(),
        ));
    }
    let mut file = step(observer, RotationStep::Create, || {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&pending)?)
    })?;
    step(observer, RotationStep::WriteHeader, || {
        file.write_all(&format::file_header())?;
        Ok(())
    })?;
    step(observer, RotationStep::SyncNew, || {
        file.sync_all()?;
        Ok(())
    })?;
    step(observer, RotationStep::Rename, || {
        fs::rename(&pending, &destination)?;
        Ok(())
    })?;
    step(observer, RotationStep::SyncDirectory, || {
        directory_file.sync_all()?;
        Ok(())
    })?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Workspace;

    const STEPS: [RotationStep; 6] = [
        RotationStep::SyncOld,
        RotationStep::Create,
        RotationStep::WriteHeader,
        RotationStep::SyncNew,
        RotationStep::Rename,
        RotationStep::SyncDirectory,
    ];

    fn options() -> SegmentedOptions {
        SegmentedOptions {
            max_segment_bytes: 28,
            sync_policy: SyncPolicy::Manual,
        }
    }

    fn workspace() -> Workspace {
        let work = Workspace::new();
        fs::create_dir(&work.log).unwrap();
        work
    }

    fn payloads(directory: &Path) -> Vec<Vec<u8>> {
        let mut reader = SegmentedReader::open(directory).unwrap();
        let mut payloads = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            payloads.push(record.payload);
        }
        payloads
    }

    #[test]
    fn rotation_barriers_precede_the_triggering_record() {
        let work = workspace();
        let (mut writer, _) = SegmentedWriter::open(&work.log, options()).unwrap();
        writer.append(b"first").unwrap();
        let mut events = Vec::new();
        let position = writer
            .append_observed(b"second", &mut |point, after| {
                events.push((point, after));
                if point == RotationStep::SyncDirectory && after {
                    assert_eq!(
                        fs::metadata(work.log.join(segment_name(1))).unwrap().len(),
                        FILE_HEADER_LEN
                    );
                    assert!(!work.log.join(STAGING).exists());
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(
            position,
            LogPosition {
                segment_id: 1,
                offset: 16
            }
        );
        let expected: Vec<_> = STEPS
            .into_iter()
            .flat_map(|point| [(point, false), (point, true)])
            .collect();
        assert_eq!(events, expected);
        writer.sync().unwrap();
        drop(writer);
        assert_eq!(payloads(&work.log), [b"first".to_vec(), b"second".to_vec()]);
    }

    #[test]
    fn failures_at_every_rotation_boundary_poison_and_recover() {
        for failure in STEPS {
            for after_failure in [false, true] {
                let work = workspace();
                let (mut writer, _) = SegmentedWriter::open(&work.log, options()).unwrap();
                writer.append(b"first").unwrap();
                let error = writer
                    .append_observed(b"unacknowledged", &mut |point, after| {
                        if (point, after) == (failure, after_failure) {
                            return Err(io::Error::other("injected publication failure").into());
                        }
                        Ok(())
                    })
                    .unwrap_err();
                assert!(matches!(error, Error::Segment { .. }));
                assert!(matches!(writer.append(b"retry"), Err(Error::Poisoned)));
                assert!(matches!(writer.sync(), Err(Error::Poisoned)));
                drop(writer);

                let published = failure == RotationStep::SyncDirectory
                    || (failure == RotationStep::Rename && after_failure);
                let had_staging = work.log.join(STAGING).exists();
                let (mut writer, report) = SegmentedWriter::open(&work.log, options()).unwrap();
                assert_eq!(
                    report.active_segment,
                    u64::from(published),
                    "{failure:?}, after={after_failure}"
                );
                assert_eq!(report.record_count, 1);
                assert_eq!(report.discarded_bytes, 0);
                assert_eq!(report.removed_staging, had_staging);
                assert_eq!(
                    writer.append(b"retry").unwrap(),
                    LogPosition {
                        segment_id: 1,
                        offset: 16
                    }
                );
                writer.sync().unwrap();
                drop(writer);
                assert_eq!(payloads(&work.log), [b"first".to_vec(), b"retry".to_vec()]);
            }
        }
    }

    #[test]
    fn failed_initial_publication_can_be_retried() {
        for failure in &STEPS[1..] {
            for after_failure in [false, true] {
                let work = workspace();
                let result =
                    SegmentedWriter::open_observed(&work.log, options(), &mut |point, after| {
                        if (point, after) == (*failure, after_failure) {
                            return Err(io::Error::other("injected initialization failure").into());
                        }
                        Ok(())
                    });
                assert!(result.is_err());
                let had_staging = work.log.join(STAGING).exists();
                let (mut writer, report) = SegmentedWriter::open(&work.log, options()).unwrap();
                assert_eq!(report.active_segment, 0);
                assert_eq!(report.record_count, 0);
                assert_eq!(report.valid_bytes, 16);
                assert_eq!(report.removed_staging, had_staging);
                assert_eq!(
                    writer.append(b"ok").unwrap(),
                    LogPosition {
                        segment_id: 0,
                        offset: 16
                    }
                );
                drop(writer);
                assert_eq!(payloads(&work.log), [b"ok".to_vec()]);
            }
        }
    }

    #[test]
    fn every_partial_staged_header_is_discarded_before_retry() {
        for cut in 0..FILE_HEADER_LEN as usize {
            let work = workspace();
            let (mut writer, _) = SegmentedWriter::open(&work.log, options()).unwrap();
            writer.append(b"first").unwrap();
            let error = writer.append_observed(b"second", &mut |point, after| {
                if point == RotationStep::WriteHeader && !after {
                    let mut file = OpenOptions::new()
                        .write(true)
                        .open(work.log.join(STAGING))?;
                    file.write_all(&format::file_header()[..cut])?;
                    return Err(io::Error::other("injected partial header write").into());
                }
                Ok(())
            });
            assert!(error.is_err());
            assert!(!work.log.join(segment_name(1)).exists());
            assert_eq!(
                fs::metadata(work.log.join(STAGING)).unwrap().len(),
                cut as u64
            );
            drop(writer);
            let (mut writer, report) = SegmentedWriter::open(&work.log, options()).unwrap();
            assert!(report.removed_staging);
            assert_eq!(
                writer.append(b"retry").unwrap(),
                LogPosition {
                    segment_id: 1,
                    offset: 16
                }
            );
            drop(writer);
            assert_eq!(payloads(&work.log), [b"first".to_vec(), b"retry".to_vec()]);
        }
    }

    #[cfg(target_os = "linux")]
    mod crash {
        use super::*;
        use std::{
            io::{BufRead, Read},
            os::unix::process::ExitStatusExt,
            process::{Child, Command, Stdio},
            sync::mpsc,
            thread,
            time::Duration,
        };

        const READY: &str = "APPENDLOG_ROTATION_READY";

        struct KillOnDrop(Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        #[test]
        fn killed_rotation_preserves_acknowledged_records_and_reopens() {
            for boundary in [
                "before-publication",
                "after-rename",
                "after-directory-sync",
                "acknowledged",
            ] {
                let work = workspace();
                let mut config = options();
                config.sync_policy = SyncPolicy::Always;
                let (mut writer, _) = SegmentedWriter::open(&work.log, config).unwrap();
                writer.append(b"first").unwrap();
                drop(writer);
                let mut process = KillOnDrop(
                    Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--ignored",
                            "--exact",
                            "segmented::tests::crash::rotation_process",
                            "--nocapture",
                            "--test-threads=1",
                        ])
                        .env("APPENDLOG_ROTATION_PATH", &work.log)
                        .env("APPENDLOG_ROTATION_BOUNDARY", boundary)
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::inherit())
                        .spawn()
                        .unwrap(),
                );
                let stdout = process.0.stdout.take().unwrap();
                let (sender, receiver) = mpsc::channel();
                let output = thread::spawn(move || {
                    let ready = io::BufReader::new(stdout)
                        .lines()
                        .any(|line| line.is_ok_and(|line| line == READY));
                    let _ = sender.send(ready);
                });
                assert!(
                    receiver
                        .recv_timeout(Duration::from_secs(30))
                        .expect("rotation handshake timed out")
                );
                output.join().unwrap();
                assert!(matches!(SegmentedReader::open(&work.log), Err(Error::Busy)));
                assert!(matches!(
                    SegmentedWriter::open(&work.log, config),
                    Err(Error::Busy)
                ));
                process.0.kill().unwrap();
                assert_eq!(process.0.wait().unwrap().signal(), Some(9));

                let acknowledged = boundary == "acknowledged";
                let expected = if acknowledged {
                    vec![b"first".to_vec(), b"second".to_vec()]
                } else {
                    vec![b"first".to_vec()]
                };
                assert_eq!(payloads(&work.log), expected);
                let (mut writer, report) = SegmentedWriter::open(&work.log, config).unwrap();
                assert_eq!(report.record_count, expected.len() as u64);
                assert_eq!(report.discarded_bytes, 0);
                assert_eq!(report.removed_staging, boundary == "before-publication");
                let position = writer.append(b"after restart").unwrap();
                assert_eq!(
                    position,
                    LogPosition {
                        segment_id: if acknowledged { 2 } else { 1 },
                        offset: 16
                    }
                );
                drop(writer);
                let mut expected = expected;
                expected.push(b"after restart".to_vec());
                assert_eq!(payloads(&work.log), expected);
            }
        }

        #[test]
        #[ignore = "subprocess helper, started by the rotation crash test"]
        fn rotation_process() {
            let path = std::env::var_os("APPENDLOG_ROTATION_PATH").unwrap();
            let boundary = std::env::var("APPENDLOG_ROTATION_BOUNDARY").unwrap();
            let mut config = options();
            config.sync_policy = SyncPolicy::Always;
            let (mut writer, _) = SegmentedWriter::open(path, config).unwrap();
            let position = writer
                .append_observed(b"second", &mut |point, after| {
                    let stop = match boundary.as_str() {
                        "before-publication" => point == RotationStep::SyncNew,
                        "after-rename" => point == RotationStep::Rename,
                        "after-directory-sync" => point == RotationStep::SyncDirectory,
                        "acknowledged" => false,
                        _ => panic!("unknown rotation boundary"),
                    };
                    if stop && after {
                        wait_for_kill();
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                position,
                LogPosition {
                    segment_id: 1,
                    offset: 16
                }
            );
            assert_eq!(boundary, "acknowledged");
            wait_for_kill();
        }

        fn wait_for_kill() -> ! {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "\n{READY}").unwrap();
            stdout.flush().unwrap();
            drop(stdout);
            io::stdin()
                .read_exact(&mut [0])
                .expect("parent keeps stdin open");
            panic!("parent should kill the writer");
        }
    }
}
