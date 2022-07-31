use std::borrow::Cow;
use std::fmt;
use std::io;

use crc::{Crc, CRC_32_ISO_HDLC as CRC32};

use serde::Deserialize;
use serde_with::{serde_as, Bytes};

use byteorder::{ByteOrder, LittleEndian};

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[serde_as]
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Copy)]
pub enum Error {
	InvalidSegmentTag(u8),
	CrcMismatch { found: u32, calculated: u32 },
	InvalidSize(SegmentType, u32),
	InvalidHeader([u8; 8]),
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::InvalidSegmentTag(tag) => write!(f, "invalid segment tag: 0x{:02x}", tag),
			Self::CrcMismatch { found, calculated } => write!(
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

#[derive(IntoPrimitive, TryFromPrimitive, Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SegmentType {
	Put = 0,
	Delete = 1,
	Commit = 2,
}

impl SegmentType {
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
pub enum Segment<'x> {
	Put { key: Id, data: Cow<'x, [u8]> },
	Delete { key: Id },
	Commit,
}

impl<'x> Segment<'x> {
	pub fn tag(&self) -> SegmentType {
		match self {
			Self::Put { .. } => SegmentType::Put,
			Self::Delete { .. } => SegmentType::Delete,
			Self::Commit { .. } => SegmentType::Commit,
		}
	}
}

pub fn read_segment<R: io::Read>(mut src: R) -> io::Result<Option<Segment<'static>>> {
	let crc_impl = Crc::<u32>::new(&CRC32);
	let mut crc_digest = crc_impl.digest();
	let mut crc_raw = [0u8; 4];
	let mut size_raw = [0u8; 4];
	let mut tag = 0u8;
	match src.read(&mut crc_raw[..])? {
		0 => return Ok(None),
		4 => (),
		_ => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "partial crc")),
	}
	src.read_exact(&mut size_raw[..])?;
	src.read_exact(std::slice::from_mut(&mut tag))?;

	crc_digest.update(&size_raw[..]);
	crc_digest.update(std::slice::from_ref(&tag));

	let crc = LittleEndian::read_u32(&crc_raw[..]);
	let size = LittleEndian::read_u32(&size_raw[..]);

	let segment_type: SegmentType = match tag.try_into() {
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
		SegmentType::Put => {
			let mut key = Id::zero();
			src.read_exact(&mut key.0[..])?;
			crc_digest.update(&key.0[..]);

			// soundness: segment_type.min_size() returns 41 for Put
			let remainder = size - 41;
			let mut buffer = Vec::new();
			buffer.resize(remainder as usize, 0u8);
			src.read_exact(&mut buffer[..])?;
			crc_digest.update(&buffer[..]);

			Segment::Put {
				key,
				data: buffer.into(),
			}
		}
		SegmentType::Delete => {
			let mut key = Id::zero();
			src.read_exact(&mut key.0[..])?;
			crc_digest.update(&key.0[..]);

			Segment::Delete { key }
		}
		SegmentType::Commit => Segment::Commit,
	};

	let crc_calculated = crc_digest.finalize();
	if crc_calculated != crc {
		return Err(Error::CrcMismatch {
			found: crc,
			calculated: crc_calculated,
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

	pub fn read(&mut self) -> io::Result<Option<Segment>> {
		self.read_header()?;
		read_segment(&mut self.inner)
	}
}

impl<R: io::Read + io::Seek> SegmentReader<R> {
	pub fn read_pos(&mut self) -> io::Result<Option<(u64, Segment)>> {
		let pos = self.inner.stream_position()?;
		self.read_header()?;
		match read_segment(&mut self.inner)? {
			Some(segment) => Ok(Some((pos, segment))),
			None => Ok(None),
		}
	}
}
