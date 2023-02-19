use std::borrow::Cow;
use std::io;
use std::ops::Range;
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

impl SegmentIndexEntry {
	fn is_empty(&self) -> bool {
		self.segment == MAGIC_VALUE_EMPTY
	}

	fn is_deleted(&self) -> bool {
		self.segment == MAGIC_VALUE_DELETED
	}

	fn matches(&self, id: &Id) -> Option<bool> {
		if self.is_empty() {
			return None;
		}
		if self.is_deleted() {
			return Some(false);
		}
		Some(self.key == *id)
	}
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
	nentries: usize,
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

		let nbuckets = header.nbuckets as usize;

		let total_data = HEADER_SIZE + nbuckets * SEGMENT_INDEX_ENTRY_SIZE;
		if map.len() < total_data {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!(
					"file is shorter ({}) than number of buckets ({}) claims ({})",
					map.len(),
					nbuckets,
					total_data,
				),
			));
		}

		Ok(Self {
			map,
			nbuckets: header.nbuckets as usize,
			nentries: header.nentries as usize,
		})
	}

	#[inline(always)]
	fn data(&self) -> &[SegmentIndexEntry] {
		unsafe {
			let start_ptr = (&self.map[HEADER_SIZE..]).as_ptr();
			// SegmentIndexEntry is packed, so transmuting from u8 is safe-ìsh.
			let start_ptr: *const SegmentIndexEntry = std::mem::transmute(start_ptr);
			// length is asserted during construction
			std::slice::from_raw_parts(start_ptr, self.nbuckets)
		}
	}

	fn derive_index(&self, id: &Id) -> usize {
		// according to _hashindex.c, the top 32 bits are used,
		// as little-endian, to index the hashmap
		let hash = LittleEndian::read_u32(&id.0[..4]) as usize;
		hash % self.nbuckets
	}

	fn search_slot(&self, id: &Id) -> Option<&SegmentIndexEntry> {
		let start = self.derive_index(id);
		let data = self.data();
		for i in 0..self.nbuckets {
			let index = (start + i) % self.nbuckets;
			let entry = &data[index];
			match entry.matches(id) {
				// match
				Some(true) => return Some(entry),
				// deleted or mismatch
				Some(false) => continue,
				// empty
				None => return None,
			}
		}
		None
	}

	pub fn iter_keys(&self) -> MmapKeyIterator<'_> {
		MmapKeyIterator {
			hashindex: self,
			inner: 0..self.nbuckets,
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

pub struct MmapKeyIterator<'x> {
	hashindex: &'x MmapSegmentIndex,
	inner: Range<usize>,
}

impl<'x> Iterator for MmapKeyIterator<'x> {
	type Item = &'x Id;

	fn next(&mut self) -> Option<Self::Item> {
		let data = self.hashindex.data();
		for index in &mut self.inner {
			let entry = &data[index];
			if entry.is_empty() || entry.is_deleted() {
				continue;
			}
			if !entry.is_deleted() {
				return Some(&entry.key);
			}
		}
		None
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.hashindex.nentries, Some(self.hashindex.nbuckets))
	}
}
