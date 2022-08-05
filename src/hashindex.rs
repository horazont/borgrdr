use std::borrow::Cow;
use std::io;
use std::path::Path;

use byteorder::{ByteOrder, LittleEndian};

use memmap::Mmap;

use super::segments::Id;

#[repr(packed)]
struct Header {
	// we treat the magic as u64, because that'll protect us on big endian
	magic: u64,
	nentries: i32,
	nbuckets: i32,
	keylen: i8,
	vallen: i8,
}

const HEADER_SIZE: usize = std::mem::size_of::<Header>();
const MAGIC: u64 = 0x5844495f47524f42;
const MAGIC_VALUE_EMPTY: u32 = 0xffffffff;
const MAGIC_VALUE_DELETED: u32 = 0xfffffffe;

#[derive(Clone, Copy, Debug)]
#[repr(packed)]
pub struct SegmentIndexEntry {
	key: Id,
	pub segment: u32,
	pub offset: u32,
	/* pub size: u32,
	pub flags: u32, */
}

const SEGMENT_INDEX_ENTRY_SIZE: usize = std::mem::size_of::<SegmentIndexEntry>();
const SEGMENT_INDEX_KEY_SIZE: usize = std::mem::size_of::<Id>();
const SEGMENT_INDEX_VALUE_SIZE: usize = SEGMENT_INDEX_ENTRY_SIZE - SEGMENT_INDEX_KEY_SIZE;

pub trait ReadSegmentIndex {
	fn get<K: AsRef<Id>>(&self, id: K) -> Option<Cow<'_, SegmentIndexEntry>>;
	fn contains<K: AsRef<Id>>(&self, id: K) -> bool;
}

pub struct MmapSegmentIndex {
	map: Mmap,
	nbuckets: usize,
}

impl MmapSegmentIndex {
	pub fn open<P: AsRef<Path>>(p: P) -> io::Result<Self> {
		let f = std::fs::File::open(p)?;
		Self::from_file(&f)
	}

	pub fn from_file(f: &std::fs::File) -> io::Result<Self> {
		let map = unsafe { Mmap::map(f)? };
		if map.len() < HEADER_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				"file too short for header",
			));
		}

		let header: &Header = unsafe {
			std::mem::transmute::<_, *const Header>((&map[..HEADER_SIZE]).as_ptr())
				.as_ref()
				.unwrap()
		};
		if header.magic != MAGIC || header.vallen < 0 || header.keylen < 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"invalid magic number; corrupt file or big endian platform",
			));
		}

		let vallen = header.vallen as usize;
		if vallen != SEGMENT_INDEX_VALUE_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!(
					"value length mismatch (not a segment index): {} != {}",
					vallen, SEGMENT_INDEX_VALUE_SIZE
				),
			));
		}

		if header.keylen as usize != SEGMENT_INDEX_KEY_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"key length mismatch (not a segment index)",
			));
		}

		if header.nentries < 0 || header.nbuckets <= 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"negative number of entries or buckets or zero buckets",
			));
		}

		Ok(Self {
			map,
			nbuckets: header.nbuckets as usize,
		})
	}

	#[inline(always)]
	fn data(&self) -> &[u8] {
		&self.map[HEADER_SIZE..]
	}

	fn derive_index(&self, id: &Id) -> usize {
		// according to _hashindex.c, the top 32 bits are used,
		// as little-endian, to index the hashmap
		let hash = LittleEndian::read_u32(&id.0[..4]) as usize;
		hash % self.nbuckets
	}

	fn search_slot(&self, id: &Id) -> Option<&SegmentIndexEntry> {
		let base_index = self.derive_index(id);
		assert!(base_index < self.nbuckets);
		let wraparound = self
			.nbuckets
			.checked_mul(SEGMENT_INDEX_ENTRY_SIZE)
			.expect("overflow while calculating hash index offsets");
		// unwrap: self.nbuckets >= base_index, thus this cannot overflow.
		let mut offset = base_index * SEGMENT_INDEX_ENTRY_SIZE;
		let start = offset;
		loop {
			let end = offset + SEGMENT_INDEX_ENTRY_SIZE;
			let entry: &'_ SegmentIndexEntry = unsafe {
				std::mem::transmute::<_, *const SegmentIndexEntry>(
					(&self.data()[offset..end]).as_ptr(),
				)
				.as_ref()
				.unwrap()
			};
			if entry.segment == MAGIC_VALUE_EMPTY {
				return None;
			}
			if entry.segment != MAGIC_VALUE_DELETED && entry.key == *id {
				return Some(entry);
			}

			offset = end % wraparound;
			if offset == start {
				return None;
			}
		}
	}
}

impl ReadSegmentIndex for MmapSegmentIndex {
	fn get<K: AsRef<Id>>(&self, id: K) -> Option<Cow<'_, SegmentIndexEntry>> {
		match self.search_slot(id.as_ref()) {
			Some(v) => Some(Cow::Borrowed(v)),
			None => None,
		}
	}

	fn contains<K: AsRef<Id>>(&self, id: K) -> bool {
		self.search_slot(id.as_ref()).is_some()
	}
}
