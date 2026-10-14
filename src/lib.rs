mod format;
mod storage;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

pub use storage::{Reader, Writer};

use std::{fmt, io};

pub const MAX_PAYLOAD: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    Always,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailKind {
    IncompleteHeader,
    IncompletePayload { expected: u32, available: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tail {
    pub offset: u64,
    pub bytes: u64,
    pub kind: TailKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inspection {
    pub version: u32,
    pub record_count: u64,
    pub file_bytes: u64,
    pub valid_bytes: u64,
    pub tail: Option<Tail>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    pub record_count: u64,
    pub valid_bytes: u64,
    pub discarded_bytes: u64,
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Busy,
    InvalidHeader(&'static str),
    Corrupt { offset: u64, reason: &'static str },
    PayloadTooLarge { len: usize },
    Poisoned,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Busy => f.write_str("log is locked by another reader or writer"),
            Self::InvalidHeader(reason) => write!(f, "invalid file header: {reason}"),
            Self::Corrupt { offset, reason } => {
                write!(f, "corrupt record at byte {offset}: {reason}")
            }
            Self::PayloadTooLarge { len } => {
                write!(f, "payload is {len} bytes; limit is {MAX_PAYLOAD}")
            }
            Self::Poisoned => f.write_str("writer failed earlier; close it and reopen the log"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
