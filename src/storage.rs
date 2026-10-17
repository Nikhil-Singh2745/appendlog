use std::{
    fs::{File, OpenOptions, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{Error, Inspection, MAX_PAYLOAD, Record, RecoveryReport, SyncPolicy, format};
use format::{FILE_HEADER_LEN, RECORD_HEADER_LEN, ReadOutcome};

pub struct Writer {
    appender: Appender<File>,
}

impl Writer {
    /// Opens or creates a log, validating it before repairing an incomplete tail.
    pub fn open(
        path: impl AsRef<Path>,
        policy: SyncPolicy,
    ) -> Result<(Self, RecoveryReport), Error> {
        let path = path.as_ref();
        let (mut file, created) = open_for_write(path)?;
        lock(file.try_lock())?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a regular file").into());
        }
        if created {
            file.write_all(&format::file_header())?;
        }

        let inspection = scan(&mut file)?;
        let discarded_bytes = inspection.file_bytes - inspection.valid_bytes;
        if discarded_bytes != 0 {
            file.set_len(inspection.valid_bytes)?;
        }
        // Reopening also establishes a barrier for complete but unacknowledged writes.
        file.sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        file.seek(SeekFrom::Start(inspection.valid_bytes))?;

        Ok((
            Self {
                appender: Appender {
                    sink: file,
                    policy,
                    next_offset: inspection.valid_bytes,
                    poisoned: false,
                },
            },
            RecoveryReport {
                record_count: inspection.record_count,
                valid_bytes: inspection.valid_bytes,
                discarded_bytes,
            },
        ))
    }

    /// Returns the record's byte offset. Durability depends on the sync policy.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, Error> {
        self.appender.append(payload)
    }

    pub fn sync(&mut self) -> Result<(), Error> {
        self.appender.sync()
    }

    pub(crate) fn from_validated(
        mut file: File,
        policy: SyncPolicy,
        next_offset: u64,
    ) -> Result<Self, Error> {
        file.seek(SeekFrom::Start(next_offset))?;
        Ok(Self {
            appender: Appender {
                sink: file,
                policy,
                next_offset,
                poisoned: false,
            },
        })
    }

    pub(crate) fn next_offset(&self) -> u64 {
        self.appender.next_offset
    }
}

pub struct Reader {
    file: File,
    inspection: Inspection,
    next_offset: u64,
    stopped: bool,
}

impl Reader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut file = File::open(path)?;
        lock(file.try_lock_shared())?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "not a regular file").into());
        }
        let inspection = scan(&mut file)?;
        file.seek(SeekFrom::Start(FILE_HEADER_LEN))?;
        Ok(Self {
            file,
            inspection,
            next_offset: FILE_HEADER_LEN,
            stopped: false,
        })
    }

    pub fn inspection(&self) -> &Inspection {
        &self.inspection
    }

    /// Reads the complete prefix. Inspect `inspection().tail` for trailing bytes.
    pub fn next_record(&mut self) -> Result<Option<Record>, Error> {
        if self.stopped {
            return Ok(None);
        }
        match format::read_record(
            &mut self.file,
            self.next_offset,
            self.inspection.valid_bytes,
        ) {
            Ok(ReadOutcome::Record(record)) => {
                self.next_offset += RECORD_HEADER_LEN + record.payload.len() as u64;
                Ok(Some(record))
            }
            Ok(ReadOutcome::End) => {
                self.stopped = true;
                Ok(None)
            }
            Ok(ReadOutcome::Tail(_)) => {
                self.stopped = true;
                Err(Error::Corrupt {
                    offset: self.next_offset,
                    reason: "previously validated prefix changed",
                })
            }
            Err(error) => {
                self.stopped = true;
                Err(error)
            }
        }
    }
}

fn open_for_write(path: &Path) -> io::Result<(File, bool)> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => Ok((file, true)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map(|file| (file, false)),
        Err(error) => Err(error),
    }
}

pub(crate) fn lock(result: Result<(), TryLockError>) -> Result<(), Error> {
    match result {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(Error::Busy),
        Err(TryLockError::Error(error)) => Err(Error::Io(error)),
    }
}

pub(crate) fn scan(file: &mut File) -> Result<Inspection, Error> {
    let file_bytes = file.metadata()?.len();
    if file_bytes < FILE_HEADER_LEN {
        return Err(Error::InvalidHeader("incomplete file header"));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; 16];
    file.read_exact(&mut header)?;
    format::validate_file_header(&header)?;

    let mut inspection = Inspection {
        version: format::VERSION,
        record_count: 0,
        file_bytes,
        valid_bytes: FILE_HEADER_LEN,
        tail: None,
    };
    loop {
        match format::read_record(file, inspection.valid_bytes, file_bytes)? {
            ReadOutcome::End => return Ok(inspection),
            ReadOutcome::Record(record) => {
                inspection.record_count += 1;
                inspection.valid_bytes += RECORD_HEADER_LEN + record.payload.len() as u64;
            }
            ReadOutcome::Tail(tail) => {
                inspection.tail = Some(tail);
                return Ok(inspection);
            }
        }
    }
}

trait DurableWrite: Write {
    fn sync(&mut self) -> io::Result<()>;
}

impl DurableWrite for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

struct Appender<S> {
    sink: S,
    policy: SyncPolicy,
    next_offset: u64,
    poisoned: bool,
}

impl<S: DurableWrite> Appender<S> {
    fn append(&mut self, payload: &[u8]) -> Result<u64, Error> {
        self.check_ready()?;
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::PayloadTooLarge { len: payload.len() });
        }
        let offset = self.next_offset;
        let next_offset = offset
            .checked_add(RECORD_HEADER_LEN + payload.len() as u64)
            .ok_or_else(|| io::Error::other("log offset overflow"))?;
        let header = format::record_header(payload);
        let result = (|| {
            self.sink.write_all(&header)?;
            self.sink.write_all(payload)?;
            if self.policy == SyncPolicy::Always {
                self.sink.sync()?;
            }
            Ok::<_, io::Error>(())
        })();
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error.into());
        }
        self.next_offset = next_offset;
        Ok(offset)
    }

    fn sync(&mut self) -> Result<(), Error> {
        self.check_ready()?;
        if let Err(error) = self.sink.sync() {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    fn check_ready(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_support as support;

    struct FaultSink {
        file: File,
        bytes_left: Option<usize>,
        fail_sync: bool,
        sync_calls: usize,
        max_write: usize,
        interrupts: usize,
    }

    impl Write for FaultSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.interrupts > 0 {
                self.interrupts -= 1;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            if self.bytes_left == Some(0) {
                return Err(io::Error::other("injected write failure"));
            }
            let limit = bytes
                .len()
                .min(self.max_write)
                .min(self.bytes_left.unwrap_or(usize::MAX));
            let written = self.file.write(&bytes[..limit])?;
            if let Some(left) = &mut self.bytes_left {
                *left -= written;
            }
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()
        }
    }

    impl DurableWrite for FaultSink {
        fn sync(&mut self) -> io::Result<()> {
            self.sync_calls += 1;
            if self.fail_sync {
                return Err(io::Error::other("injected sync failure"));
            }
            self.file.sync_all()
        }
    }

    fn appender(work: &support::Workspace, policy: SyncPolicy) -> Appender<FaultSink> {
        let (writer, _) = Writer::open(&work.log, policy).unwrap();
        Appender {
            sink: FaultSink {
                file: writer.appender.sink,
                bytes_left: None,
                fail_sync: false,
                sync_calls: 0,
                max_write: usize::MAX,
                interrupts: 0,
            },
            policy,
            next_offset: FILE_HEADER_LEN,
            poisoned: false,
        }
    }

    #[test]
    fn partial_write_failures_poison_the_writer_and_recover_on_reopen() {
        let payload = b"some bytes";
        for cut in 0..12 + payload.len() {
            let work = support::Workspace::new();
            let mut writer = appender(&work, SyncPolicy::Always);
            writer.sink.bytes_left = Some(cut);
            assert!(
                matches!(writer.append(payload), Err(Error::Io(_))),
                "cut {cut}"
            );
            assert!(matches!(writer.append(b"retry"), Err(Error::Poisoned)));
            assert!(matches!(writer.sync(), Err(Error::Poisoned)));
            assert_eq!(writer.sink.sync_calls, 0);
            assert_eq!(std::fs::metadata(&work.log).unwrap().len(), 16 + cut as u64);
            drop(writer);

            let (mut reopened, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
            assert_eq!(report.record_count, 0);
            assert_eq!(report.discarded_bytes, cut as u64);
            assert_eq!(reopened.append(b"retry").unwrap(), 16);
            drop(reopened);
            let mut reader = Reader::open(&work.log).unwrap();
            assert_eq!(reader.next_record().unwrap().unwrap().payload, b"retry");
        }
    }

    #[test]
    fn failed_sync_may_leave_a_complete_record() {
        let work = support::Workspace::new();
        let mut writer = appender(&work, SyncPolicy::Always);
        writer.sink.fail_sync = true;
        assert!(matches!(writer.append(b"ambiguous"), Err(Error::Io(_))));
        assert_eq!(writer.next_offset, 16);
        assert!(matches!(writer.append(b"retry"), Err(Error::Poisoned)));
        assert!(matches!(writer.sync(), Err(Error::Poisoned)));
        assert_eq!(writer.sink.sync_calls, 1);
        drop(writer);

        let (writer, report) = Writer::open(&work.log, SyncPolicy::Always).unwrap();
        assert_eq!(report.record_count, 1);
        assert_eq!(report.discarded_bytes, 0);
        drop(writer);
        assert_eq!(
            Reader::open(&work.log)
                .unwrap()
                .next_record()
                .unwrap()
                .unwrap()
                .payload,
            b"ambiguous"
        );
    }

    #[test]
    fn manual_policy_syncs_only_when_requested_and_poisoning_sticks() {
        let work = support::Workspace::new();
        let mut writer = appender(&work, SyncPolicy::Manual);
        writer.append(b"one").unwrap();
        writer.append(b"two").unwrap();
        assert_eq!(writer.sink.sync_calls, 0);
        writer.sync().unwrap();
        assert_eq!(writer.sink.sync_calls, 1);
        writer.sink.fail_sync = true;
        assert!(matches!(writer.sync(), Err(Error::Io(_))));
        assert!(matches!(writer.append(b"three"), Err(Error::Poisoned)));
        assert!(matches!(writer.sync(), Err(Error::Poisoned)));
        assert_eq!(writer.sink.sync_calls, 2);
    }

    #[test]
    fn short_and_interrupted_writes_are_retried_before_acknowledgment() {
        let work = support::Workspace::new();
        let mut writer = appender(&work, SyncPolicy::Always);
        writer.sink.max_write = 1;
        writer.sink.interrupts = 2;
        assert_eq!(writer.append(b"one").unwrap(), 16);
        assert_eq!(writer.append(b"two").unwrap(), 31);
        assert_eq!(writer.sink.sync_calls, 2);
        drop(writer);
        let mut reader = Reader::open(&work.log).unwrap();
        assert_eq!(reader.next_record().unwrap().unwrap().payload, b"one");
        assert_eq!(reader.next_record().unwrap().unwrap().payload, b"two");
        assert!(reader.next_record().unwrap().is_none());
    }
}
