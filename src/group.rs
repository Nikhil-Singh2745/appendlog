use std::{
    fmt, io,
    path::Path,
    sync::{Arc, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{Error, MAX_PAYLOAD, RecoveryReport, SyncPolicy, Writer};

#[derive(Debug, Clone, Copy)]
pub struct GroupOptions {
    pub queue_capacity: usize,
    pub max_batch_records: usize,
    pub max_delay: Duration,
}

impl Default for GroupOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 128,
            max_batch_records: 64,
            max_delay: Duration::from_millis(2),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CommitError {
    Storage(Arc<Error>),
    Closed,
    WorkerPanicked,
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(f),
            Self::Closed => f.write_str("group writer is closed; commit outcome may be unknown"),
            Self::WorkerPanicked => f.write_str("group writer thread panicked"),
        }
    }
}

impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<Error> for CommitError {
    fn from(error: Error) -> Self {
        Self::Storage(Arc::new(error))
    }
}

#[must_use = "wait for the receipt to confirm the sync barrier"]
pub struct Commit {
    result: mpsc::Receiver<Result<u64, CommitError>>,
}

impl Commit {
    /// Success means the record passed the batch's sync barrier.
    pub fn wait(self) -> Result<u64, CommitError> {
        self.result.recv().unwrap_or(Err(CommitError::Closed))
    }
}

#[derive(Clone)]
pub struct GroupAppender {
    sender: mpsc::SyncSender<Request>,
}

impl GroupAppender {
    /// Blocks when the queue is full. Enqueuing alone does not acknowledge a write.
    pub fn submit(&self, payload: &[u8]) -> Result<Commit, CommitError> {
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::PayloadTooLarge { len: payload.len() }.into());
        }
        let (reply, result) = mpsc::channel();
        self.sender
            .send(Request::Append(Pending {
                payload: payload.to_vec(),
                reply,
            }))
            .map_err(|_| CommitError::Closed)?;
        Ok(Commit { result })
    }
}

pub struct GroupWriter {
    appender: GroupAppender,
    worker: Option<JoinHandle<Result<(), CommitError>>>,
}

impl GroupWriter {
    pub fn open(
        path: impl AsRef<Path>,
        options: GroupOptions,
    ) -> Result<(Self, RecoveryReport), Error> {
        if options.queue_capacity == 0 || options.max_batch_records == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue capacity and batch size must be positive",
            )
            .into());
        }
        let (writer, recovery) = Writer::open(path, SyncPolicy::Manual)?;
        let (sender, receiver) = mpsc::sync_channel(options.queue_capacity);
        let worker = thread::Builder::new()
            .name("appendlog-group".into())
            .spawn(move || run(writer, receiver, options))?;
        Ok((
            Self {
                appender: GroupAppender { sender },
                worker: Some(worker),
            },
            recovery,
        ))
    }

    pub fn appender(&self) -> GroupAppender {
        self.appender.clone()
    }

    /// Commits requests queued before the shutdown marker, then releases the file.
    pub fn shutdown(mut self) -> Result<(), CommitError> {
        self.close()
    }

    fn close(&mut self) -> Result<(), CommitError> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        let _ = self.appender.sender.send(Request::Shutdown);
        worker.join().unwrap_or(Err(CommitError::WorkerPanicked))
    }
}

impl Drop for GroupWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct Pending {
    payload: Vec<u8>,
    reply: mpsc::Sender<Result<u64, CommitError>>,
}

enum Request {
    Append(Pending),
    Shutdown,
}

trait CommitSink {
    fn append(&mut self, payload: &[u8]) -> Result<u64, Error>;
    fn sync(&mut self) -> Result<(), Error>;
}

impl CommitSink for Writer {
    fn append(&mut self, payload: &[u8]) -> Result<u64, Error> {
        Writer::append(self, payload)
    }

    fn sync(&mut self) -> Result<(), Error> {
        Writer::sync(self)
    }
}

fn run(
    mut writer: impl CommitSink,
    receiver: mpsc::Receiver<Request>,
    options: GroupOptions,
) -> Result<(), CommitError> {
    loop {
        let first = match receiver.recv() {
            Ok(Request::Append(pending)) => pending,
            Ok(Request::Shutdown) | Err(_) => return Ok(()),
        };
        let started = Instant::now();
        let mut batch = vec![first];
        let mut shutdown = false;
        while batch.len() < options.max_batch_records {
            let remaining = options.max_delay.saturating_sub(started.elapsed());
            match receiver.recv_timeout(remaining) {
                Ok(Request::Append(pending)) => batch.push(pending),
                Ok(Request::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    shutdown = true;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
            }
            if started.elapsed() >= options.max_delay {
                break;
            }
        }
        commit_batch(&mut writer, batch)?;
        if shutdown {
            return Ok(());
        }
    }
}

fn commit_batch(writer: &mut impl CommitSink, batch: Vec<Pending>) -> Result<(), CommitError> {
    let result = (|| {
        let mut offsets = Vec::with_capacity(batch.len());
        for pending in &batch {
            offsets.push(writer.append(&pending.payload)?);
        }
        // No reply escapes before the whole batch has crossed the barrier.
        writer.sync()?;
        Ok::<_, Error>(offsets)
    })();
    match result {
        Ok(offsets) => {
            for (pending, offset) in batch.into_iter().zip(offsets) {
                let _ = pending.reply.send(Ok(offset));
            }
            Ok(())
        }
        Err(error) => {
            let error = CommitError::from(error);
            for pending in batch {
                let _ = pending.reply.send(Err(error.clone()));
            }
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct State {
        payloads: Vec<Vec<u8>>,
        next_offset: u64,
        syncs: usize,
    }

    #[derive(Default)]
    struct Sink {
        state: Arc<Mutex<State>>,
        fail_append: Option<usize>,
        fail_sync: bool,
        sync_gate: Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
    }

    impl CommitSink for Sink {
        fn append(&mut self, payload: &[u8]) -> Result<u64, Error> {
            let mut state = self.state.lock().unwrap();
            if self.fail_append == Some(state.payloads.len()) {
                return Err(io::Error::other("injected append failure").into());
            }
            let offset = state.next_offset;
            state.next_offset += 12 + payload.len() as u64;
            state.payloads.push(payload.to_vec());
            Ok(offset)
        }

        fn sync(&mut self) -> Result<(), Error> {
            self.state.lock().unwrap().syncs += 1;
            if let Some((entered, release)) = &self.sync_gate {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(30)).unwrap();
            }
            if self.fail_sync {
                return Err(io::Error::other("injected sync failure").into());
            }
            Ok(())
        }
    }

    fn pending(payload: &[u8]) -> (Pending, Commit) {
        let (reply, result) = mpsc::channel();
        (
            Pending {
                payload: payload.to_vec(),
                reply,
            },
            Commit { result },
        )
    }

    fn queued(
        count: u8,
    ) -> (
        mpsc::SyncSender<Request>,
        mpsc::Receiver<Request>,
        Vec<Commit>,
    ) {
        let (sender, receiver) = mpsc::sync_channel(count as usize + 1);
        let mut commits = Vec::new();
        for value in 0..count {
            let (pending, commit) = pending(&[value]);
            sender.send(Request::Append(pending)).unwrap();
            commits.push(commit);
        }
        (sender, receiver, commits)
    }

    fn options(batch_size: usize) -> GroupOptions {
        GroupOptions {
            queue_capacity: 8,
            max_batch_records: batch_size,
            max_delay: Duration::from_secs(60),
        }
    }

    #[test]
    fn receipts_stay_pending_until_the_shared_sync_finishes() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let mut sink = Sink {
            sync_gate: Some((entered_tx, release_rx)),
            ..Sink::default()
        };
        let state = sink.state.clone();
        let (batch, commits): (Vec<_>, Vec<_>) = (0..3).map(|value| pending(&[value])).unzip();
        let worker = thread::spawn(move || commit_batch(&mut sink, batch));
        entered_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        for commit in &commits {
            assert!(matches!(
                commit.result.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
        }
        release_tx.send(()).unwrap();
        for (index, commit) in commits.into_iter().enumerate() {
            assert_eq!(commit.wait().unwrap(), index as u64 * 13);
        }
        worker.join().unwrap().unwrap();
        assert_eq!(state.lock().unwrap().syncs, 1);
    }

    #[test]
    fn batch_limit_and_shutdown_flush_share_barriers() {
        let (sender, receiver, commits) = queued(7);
        sender.send(Request::Shutdown).unwrap();
        let sink = Sink::default();
        let state = sink.state.clone();
        run(sink, receiver, options(3)).unwrap();
        for (index, commit) in commits.into_iter().enumerate() {
            assert_eq!(commit.wait().unwrap(), index as u64 * 13);
        }
        let state = state.lock().unwrap();
        assert_eq!(state.syncs, 3);
        assert_eq!(
            state.payloads,
            (0..7).map(|value| vec![value]).collect::<Vec<_>>()
        );
    }

    #[test]
    fn sync_failure_rejects_the_batch_and_closes_queued_receipts() {
        let (_sender, receiver, mut commits) = queued(3);
        let sink = Sink {
            fail_sync: true,
            ..Sink::default()
        };
        let state = sink.state.clone();
        let failure = run(sink, receiver, options(2)).unwrap_err();
        let CommitError::Storage(original) = failure else {
            panic!("expected storage error")
        };
        let queued = commits.pop().unwrap();
        for commit in commits {
            let Err(CommitError::Storage(error)) = commit.wait() else {
                panic!("expected failed batch")
            };
            assert!(Arc::ptr_eq(&original, &error));
        }
        assert!(matches!(queued.wait(), Err(CommitError::Closed)));
        let state = state.lock().unwrap();
        assert_eq!(state.payloads.len(), 2);
        assert_eq!(state.syncs, 1);
    }

    #[test]
    fn append_failure_rejects_even_the_unattempted_records() {
        let (batch, commits): (Vec<_>, Vec<_>) = (0..3).map(|value| pending(&[value])).unzip();
        let mut sink = Sink {
            fail_append: Some(1),
            ..Sink::default()
        };
        assert!(matches!(
            commit_batch(&mut sink, batch),
            Err(CommitError::Storage(_))
        ));
        for commit in commits {
            assert!(matches!(commit.wait(), Err(CommitError::Storage(_))));
        }
        let state = sink.state.lock().unwrap();
        assert_eq!(state.payloads.len(), 1);
        assert_eq!(state.syncs, 0);
    }

    #[test]
    fn dropping_a_receipt_does_not_cancel_its_record() {
        let (first, abandoned) = pending(b"first");
        let (second, commit) = pending(b"second");
        drop(abandoned);
        let mut sink = Sink::default();
        commit_batch(&mut sink, vec![first, second]).unwrap();
        assert_eq!(commit.wait().unwrap(), 17);
        let state = sink.state.lock().unwrap();
        assert_eq!(state.payloads, [b"first".to_vec(), b"second".to_vec()]);
        assert_eq!(state.syncs, 1);
    }

    #[test]
    fn disconnection_flushes_a_partial_batch() {
        let (sender, receiver, commits) = queued(2);
        drop(sender);
        let sink = Sink::default();
        let state = sink.state.clone();
        run(sink, receiver, options(8)).unwrap();
        for commit in commits {
            commit.wait().unwrap();
        }
        assert_eq!(state.lock().unwrap().syncs, 1);
    }

    #[test]
    fn idle_shutdown_does_not_sync_an_empty_batch() {
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.send(Request::Shutdown).unwrap();
        let sink = Sink::default();
        let state = sink.state.clone();
        run(sink, receiver, options(8)).unwrap();
        assert_eq!(state.lock().unwrap().syncs, 0);
    }

    #[test]
    fn low_traffic_flushes_without_a_shutdown_marker() {
        let (sender, receiver, mut commits) = queued(1);
        let mut options = options(8);
        options.max_delay = Duration::from_millis(1);
        let worker = thread::spawn(move || run(Sink::default(), receiver, options));
        assert_eq!(
            commits
                .pop()
                .unwrap()
                .result
                .recv_timeout(Duration::from_secs(30))
                .unwrap()
                .unwrap(),
            0,
        );
        drop(sender);
        worker.join().unwrap().unwrap();
    }

    #[test]
    fn shutdown_reports_a_worker_storage_failure() {
        let (sender, receiver) = mpsc::sync_channel(4);
        let sink = Sink {
            fail_sync: true,
            ..Sink::default()
        };
        let worker = thread::spawn(move || run(sink, receiver, options(8)));
        let writer = GroupWriter {
            appender: GroupAppender { sender },
            worker: Some(worker),
        };
        let appender = writer.appender();
        let commit = appender.submit(b"ambiguous").unwrap();
        assert!(matches!(writer.shutdown(), Err(CommitError::Storage(_))));
        assert!(matches!(commit.wait(), Err(CommitError::Storage(_))));
        assert!(matches!(
            appender.submit(b"retry"),
            Err(CommitError::Closed)
        ));
    }

    #[test]
    fn shutdown_reports_a_panicked_worker() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let _request = receiver.recv().unwrap();
            panic!("injected worker panic");
        });
        let writer = GroupWriter {
            appender: GroupAppender { sender },
            worker: Some(worker),
        };
        let commit = writer.appender().submit(b"unknown outcome").unwrap();
        assert!(matches!(
            writer.shutdown(),
            Err(CommitError::WorkerPanicked)
        ));
        assert!(matches!(commit.wait(), Err(CommitError::Closed)));
    }

    #[test]
    fn requests_after_the_shutdown_marker_are_not_written() {
        let (sender, receiver) = mpsc::sync_channel(3);
        let (first, committed) = pending(b"before");
        let (second, rejected) = pending(b"after");
        sender.send(Request::Append(first)).unwrap();
        sender.send(Request::Shutdown).unwrap();
        sender.send(Request::Append(second)).unwrap();
        let sink = Sink::default();
        let state = sink.state.clone();
        run(sink, receiver, options(8)).unwrap();
        committed.wait().unwrap();
        assert!(matches!(rejected.wait(), Err(CommitError::Closed)));
        assert_eq!(state.lock().unwrap().payloads, [b"before".to_vec()]);
    }
}
