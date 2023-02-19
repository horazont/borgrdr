use std::borrow::Cow;
use std::fmt;
use std::io;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

use byteorder::{ByteOrder, LittleEndian};

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[serde_as]
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id(#[serde_as(as = "Bytes")] pub [u8; 32]);

impl Id {
	pub fn zero() -> Self {
		Self([0u8; 32])
	}
}

impl AsRef<Id> for Id {
	fn as_ref(&self) -> &Id {
		self
	}
}

impl fmt::Debug for Id {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		for (i, b) in self.0.iter().enumerate() {
			write!(f, "{:02x}", b)?;
			if i % 4 == 3 && i != 31 {
				write!(f, "-")?;
			}
		}
		write!(f, "_id")
	}
}

impl FromStr for Id {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.len() != 64 {
			return Err(());
		}

		let mut result = [0u8; 32];
		for i in 0..32 {
			let byte_str = &s[i * 2..i * 2 + 2];
			let byte = match u8::from_str_radix(byte_str, 16) {
				Ok(v) => v,
				Err(_) => return Err(()),
			};
			result[i] = byte;
		}

		Ok(Self(result))
	}
}

#[derive(Debug, Clone)]
pub enum Error {
	InvalidSegmentTag(u8),
	CrcMismatch {
		found: u32,
		calculated: u32,
		unchecked: LogEntry<'static>,
	},
	InvalidSize(LogEntryType, u32),
	InvalidHeader([u8; 8]),
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::InvalidSegmentTag(tag) => write!(f, "invalid segment tag: 0x{:02x}", tag),
			Self::CrcMismatch {
				found, calculated, ..
			} => write!(
				f,
				"crc mismatch: 0x{:08x} (found) != 0x{:08x} (calculated)",
				found, calculated
			),
			Self::InvalidSize(segment_type, size) => write!(
				f,
				"size {} is invalid for segment of type {:?}",
				size, segment_type
			),
			Self::InvalidHeader(hdr) => write!(f, "invalid segment header: {:?}", hdr),
		}
	}
}

impl std::error::Error for Error {}

impl From<Error> for io::Error {
	fn from(other: Error) -> io::Error {
		io::Error::new(io::ErrorKind::InvalidData, other)
	}
}

impl TryFrom<io::Error> for Error {
	type Error = io::Error;

	fn try_from(other: io::Error) -> Result<Self, Self::Error> {
		if other.kind() != io::ErrorKind::InvalidData {
			return Err(other);
		}
		let inner: Option<&Self> = other.get_ref().and_then(|x| x.downcast_ref());
		if inner.is_some() {
			drop(inner);
			Ok(*other.into_inner().unwrap().downcast().unwrap())
		} else {
			return Err(other);
		}
	}
}

#[derive(IntoPrimitive, TryFromPrimitive, Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum LogEntryType {
	Put = 0,
	Delete = 1,
	Commit = 2,
}

impl LogEntryType {
	fn min_size(&self) -> u32 {
		match self {
			Self::Put | Self::Delete => 41,
			Self::Commit => 9,
		}
	}

	fn max_size(&self) -> Option<u32> {
		match self {
			Self::Put => None,
			Self::Delete => Some(41),
			Self::Commit => Some(9),
		}
	}
}

#[derive(Debug, Clone)]
pub enum LogEntry<'x> {
	Put { key: Id, data: Cow<'x, [u8]> },
	Delete { key: Id },
	Commit,
}

impl<'x> LogEntry<'x> {
	pub fn tag(&self) -> LogEntryType {
		match self {
			Self::Put { .. } => LogEntryType::Put,
			Self::Delete { .. } => LogEntryType::Delete,
			Self::Commit { .. } => LogEntryType::Commit,
		}
	}
}

fn try_read_upto<R: io::Read>(mut src: R, out: &mut [u8]) -> io::Result<usize> {
	let mut offs = 0;
	loop {
		let n = match src.read(&mut out[offs..]) {
			Ok(0) => return Ok(offs),
			Err(e) => return Err(e),
			Ok(n) => n,
		};
		offs += n;
		if offs == out.len() {
			return Ok(offs);
		}
	}
}

const COMMIT_LOG_ENTRY: [u8; 9] = *b"\x40\xf4\x3c\x25\x09\x00\x00\x00\x02";

pub fn read_segment<R: io::Read>(mut src: R) -> io::Result<Option<LogEntry<'static>>> {
	let mut main_buffer = [0u8; 9];
	match try_read_upto(&mut src, &mut main_buffer[..])? {
		0 => return Ok(None),
		9 => (),
		_ => {
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"partial segment",
			))
		}
	}

	if main_buffer == COMMIT_LOG_ENTRY {
		return Ok(Some(LogEntry::Commit));
	}

	let mut crc_digest = crc32fast::Hasher::new();
	let crc_raw = &main_buffer[..4];
	let size_raw = &main_buffer[4..8];
	let tag = main_buffer[8];

	crc_digest.update(&size_raw[..]);
	crc_digest.update(std::slice::from_ref(&tag));

	let crc = LittleEndian::read_u32(&crc_raw[..]);
	let size = LittleEndian::read_u32(&size_raw[..]);

	let segment_type: LogEntryType = match tag.try_into() {
		Ok(v) => v,
		Err(_) => return Err(Error::InvalidSegmentTag(tag).into()),
	};

	if size < segment_type.min_size() {
		return Err(Error::InvalidSize(segment_type, size).into());
	}

	if let Some(max_size) = segment_type.max_size() {
		if size > max_size {
			return Err(Error::InvalidSize(segment_type, size).into());
		}
	}

	let result = match segment_type {
		LogEntryType::Put => {
			let mut key = Id::zero();
			src.read_exact(&mut key.0[..])?;
			crc_digest.update(&key.0[..]);

			// soundness: segment_type.min_size() returns 41 for Put
			let remainder = size - 41;
			let mut buffer = Vec::new();
			buffer.resize(remainder as usize, 0u8);
			src.read_exact(&mut buffer[..])?;
			crc_digest.update(&buffer[..]);

			LogEntry::Put {
				key,
				data: buffer.into(),
			}
		}
		LogEntryType::Delete => {
			let mut key = Id::zero();
			src.read_exact(&mut key.0[..])?;
			crc_digest.update(&key.0[..]);

			LogEntry::Delete { key }
		}
		LogEntryType::Commit => LogEntry::Commit,
	};

	let crc_calculated = crc_digest.finalize();
	if crc_calculated != crc {
		return Err(Error::CrcMismatch {
			found: crc,
			calculated: crc_calculated,
			unchecked: result,
		}
		.into());
	}

	Ok(Some(result))
}

pub struct SegmentReader<R> {
	inner: R,
	had_header: bool,
}

impl<R: io::Read> SegmentReader<R> {
	pub fn new(inner: R) -> Self {
		Self {
			inner,
			had_header: false,
		}
	}

	fn read_header(&mut self) -> io::Result<()> {
		if self.had_header {
			return Ok(());
		}
		let mut buffer = [0u8; 8];
		self.inner.read_exact(&mut buffer[..])?;
		self.had_header = true;
		if &buffer[..] != b"BORG_SEG" {
			return Err(Error::InvalidHeader(buffer).into());
		}
		Ok(())
	}

	pub fn read(&mut self) -> io::Result<Option<LogEntry<'static>>> {
		self.read_header()?;
		read_segment(&mut self.inner)
	}
}

impl<R: io::Read + io::Seek> SegmentReader<R> {
	pub fn read_pos(&mut self) -> (u64, io::Result<Option<LogEntry<'static>>>) {
		match self.read_header() {
			Ok(()) => (),
			Err(e) => return (0, Err(e)),
		}
		let pos = self
			.inner
			.stream_position()
			.expect("stream position not known");
		(pos, read_segment(&mut self.inner))
	}
}
