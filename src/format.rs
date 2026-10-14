use std::io::Read;

use crate::{Error, MAX_PAYLOAD, Record, Tail, TailKind};

pub(crate) const FILE_HEADER_LEN: u64 = 16;
pub(crate) const RECORD_HEADER_LEN: u64 = 12;
pub(crate) const VERSION: u32 = 1;
const MAGIC: &[u8; 8] = b"APNDLOG\0";

pub(crate) fn file_header() -> [u8; 16] {
    let mut header = [0; 16];
    header[..8].copy_from_slice(MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header
}

pub(crate) fn validate_file_header(header: &[u8; 16]) -> Result<(), Error> {
    if &header[..8] != MAGIC {
        return Err(Error::InvalidHeader("magic does not match"));
    }
    if word(&header[8..12]) != VERSION {
        return Err(Error::InvalidHeader("unsupported version"));
    }
    if header[12..] != [0; 4] {
        return Err(Error::InvalidHeader("reserved bytes are nonzero"));
    }
    Ok(())
}

pub(crate) fn record_header(payload: &[u8]) -> [u8; 12] {
    let mut header = [0; 12];
    header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[4..8].copy_from_slice(&crc32(payload).to_le_bytes());
    let checksum = crc32(&header[..8]);
    header[8..].copy_from_slice(&checksum.to_le_bytes());
    header
}

pub(crate) enum ReadOutcome {
    End,
    Record(Record),
    Tail(Tail),
}

pub(crate) fn read_record(
    source: &mut impl Read,
    offset: u64,
    file_bytes: u64,
) -> Result<ReadOutcome, Error> {
    let remaining = file_bytes - offset;
    if remaining == 0 {
        return Ok(ReadOutcome::End);
    }
    if remaining < RECORD_HEADER_LEN {
        return Ok(ReadOutcome::Tail(Tail {
            offset,
            bytes: remaining,
            kind: TailKind::IncompleteHeader,
        }));
    }

    let mut header = [0; 12];
    source.read_exact(&mut header)?;
    if crc32(&header[..8]) != word(&header[8..]) {
        return Err(Error::Corrupt {
            offset,
            reason: "header checksum mismatch",
        });
    }
    let length = word(&header[..4]);
    if length as usize > MAX_PAYLOAD {
        return Err(Error::Corrupt {
            offset,
            reason: "payload length exceeds limit",
        });
    }

    let available = remaining - RECORD_HEADER_LEN;
    if available < u64::from(length) {
        return Ok(ReadOutcome::Tail(Tail {
            offset,
            bytes: remaining,
            kind: TailKind::IncompletePayload {
                expected: length,
                available,
            },
        }));
    }

    let mut payload = vec![0; length as usize];
    source.read_exact(&mut payload)?;
    if crc32(&payload) != word(&header[4..8]) {
        return Err(Error::Corrupt {
            offset,
            reason: "payload checksum mismatch",
        });
    }
    Ok(ReadOutcome::Record(Record { offset, payload }))
}

fn word(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("four-byte format field"))
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xedb8_8320 } else { 0 };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

const CRC_TABLE: [u32; 256] = crc_table();

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &byte in bytes {
        crc = CRC_TABLE[((crc as u8) ^ byte) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ieee_crc_vectors() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414f_a339
        );
    }
}
