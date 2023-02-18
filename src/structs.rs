use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::hash;
use std::hash::Hash;

use serde::de;
use serde::Deserialize;

use bytes::Bytes;

use super::segments::Id;

#[derive(Deserialize, Debug)]
pub struct ManifestArchiveEntry {
	id: Id,
	time: String,
}

impl ManifestArchiveEntry {
	pub fn id(&self) -> &Id {
		&self.id
	}

	pub fn timestamp(&self) -> &str {
		&self.time
	}
}

#[derive(Deserialize, Debug)]
pub struct Manifest {
	version: u8,
	archives: HashMap<String, ManifestArchiveEntry>,
	timestamp: String,
	item_keys: Vec<String>,
	config: HashMap<String, rmpv::Value>,
}

impl Manifest {
	pub fn version(&self) -> u8 {
		self.version
	}

	pub fn archives(&self) -> &HashMap<String, ManifestArchiveEntry> {
		&self.archives
	}

	pub fn archive<K: Hash + Eq>(&self, name: &K) -> Option<&ManifestArchiveEntry>
	where
		String: Borrow<K>,
	{
		self.archives.get(name)
	}

	pub fn timestamp(&self) -> &str {
		&self.timestamp
	}

	pub fn item_keys(&self) -> &[String] {
		&self.item_keys
	}

	pub fn config(&self) -> &HashMap<String, rmpv::Value> {
		&self.config
	}
}

enum ChunkerParamsDiscriminator {
	BuzhashImplied(u64),
	BuzhashExplicit,
	Fixed,
}

struct ChunkerParamsDiscriminatorVisitor();

impl<'de> de::Visitor<'de> for ChunkerParamsDiscriminatorVisitor {
	type Value = ChunkerParamsDiscriminator;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("`buzhash`, `fixed`, or a u64")
	}

	fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
		match s {
			"buzhash" => Ok(Self::Value::BuzhashExplicit),
			"fixed" => Ok(Self::Value::Fixed),
			_ => Err(E::invalid_value(
				de::Unexpected::Str(s),
				&"buzhash or fixed",
			)),
		}
	}

	fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
		Ok(Self::Value::BuzhashImplied(v))
	}
}

impl<'de> de::Deserialize<'de> for ChunkerParamsDiscriminator {
	fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserializer.deserialize_str(ChunkerParamsDiscriminatorVisitor())
	}
}

#[derive(Debug)]
pub enum ChunkerParams {
	Buzhash {
		chunk_min_exp: u64,
		chunk_max_exp: u64,
		hash_mask_bits: u64,
		hash_window_size: u64,
	},
	Fixed {
		size: u64,
		hdr_size: Option<u64>,
	},
}

struct ChunkerParamsVisitor();

impl<'de> de::Visitor<'de> for ChunkerParamsVisitor {
	type Value = ChunkerParams;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("tuple struct ChunkerParams")
	}

	fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
		let discriminator: ChunkerParamsDiscriminator = match seq.next_element()? {
			Some(v) => v,
			None => return Err(de::Error::invalid_length(0, &"2, 3, 4, or 5")),
		};
		let (offs, chunk_min_exp) = match discriminator {
			ChunkerParamsDiscriminator::BuzhashImplied(chunk_min_exp) => (0, chunk_min_exp),
			ChunkerParamsDiscriminator::BuzhashExplicit => (
				1,
				seq.next_element()?
					.ok_or(de::Error::invalid_length(1, &"5"))?,
			),
			ChunkerParamsDiscriminator::Fixed => {
				let size: u64 = seq
					.next_element()?
					.ok_or(de::Error::invalid_length(1, &"2 or 3"))?;
				let hdr_size: Option<u64> = seq.next_element()?;
				return Ok(ChunkerParams::Fixed { size, hdr_size });
			}
		};
		let chunk_max_exp: u64 = seq
			.next_element()?
			.ok_or(de::Error::invalid_length(offs + 1, &"4/5"))?;
		let hash_mask_bits: u64 = seq
			.next_element()?
			.ok_or(de::Error::invalid_length(offs + 2, &"4/5"))?;
		let hash_window_size: u64 = seq
			.next_element()?
			.ok_or(de::Error::invalid_length(offs + 3, &"4/5"))?;
		Ok(ChunkerParams::Buzhash {
			chunk_max_exp,
			chunk_min_exp,
			hash_mask_bits,
			hash_window_size,
		})
	}
}

impl<'de> de::Deserialize<'de> for ChunkerParams {
	fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserializer.deserialize_seq(ChunkerParamsVisitor())
	}
}

#[derive(Deserialize, Debug)]
pub struct Archive {
	version: u8,
	name: String,
	cmdline: Vec<String>,
	hostname: String,
	username: String,
	time: String,
	time_end: String,
	comment: Option<String>,
	chunker_params: Option<ChunkerParams>,
	pub(crate) items: Vec<Id>,
}

impl Archive {
	pub fn version(&self) -> u8 {
		self.version
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn cmdline(&self) -> &[String] {
		&self.cmdline
	}

	pub fn hostname(&self) -> &str {
		&self.hostname
	}

	pub fn username(&self) -> &str {
		&self.username
	}

	pub fn start_time(&self) -> &str {
		&self.time
	}

	pub fn end_time(&self) -> &str {
		&self.time_end
	}

	pub fn chunker_params(&self) -> Option<&ChunkerParams> {
		self.chunker_params.as_ref()
	}

	pub fn comment(&self) -> Option<&str> {
		self.comment.as_ref().map(|x| x.as_str())
	}

	pub fn items(&self) -> &[Id] {
		&self.items
	}
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct Chunk(Id, u64, u64);

impl PartialEq<Chunk> for Chunk {
	fn eq(&self, other: &Chunk) -> bool {
		self.0 == other.0
	}
}

impl Eq for Chunk {}

impl Hash for Chunk {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		self.0.hash(state)
	}
}

impl Chunk {
	pub fn id(&self) -> &Id {
		&self.0
	}

	pub fn size(&self) -> u64 {
		self.1
	}

	pub fn csize(&self) -> u64 {
		self.2
	}
}

fn empty_bytes_hashmap() -> HashMap<Bytes, Bytes> {
	HashMap::new()
}

// TODOC: names!
#[derive(Deserialize, Debug)]
pub struct ArchiveItem {
	path: Bytes,
	mode: u32,
	uid: u32,
	gid: u32,
	user: Option<Bytes>,
	group: Option<Bytes>,
	atime: Option<i64>,
	ctime: Option<i64>,
	mtime: Option<i64>,
	birthtime: Option<i64>,
	size: Option<u64>,
	acl_access: Option<Bytes>,
	acl_default: Option<Bytes>,
	#[serde(default = "empty_bytes_hashmap")]
	xattrs: HashMap<Bytes, Bytes>,
	chunks: Option<Vec<Chunk>>,
	source: Option<Bytes>,
}

impl ArchiveItem {
	pub fn path(&self) -> &Bytes {
		&self.path
	}

	pub fn mode(&self) -> u32 {
		self.mode
	}

	pub fn uid(&self) -> u32 {
		self.uid
	}

	pub fn gid(&self) -> u32 {
		self.gid
	}

	pub fn user(&self) -> Option<&[u8]> {
		self.user.as_ref().map(|x| &x[..])
	}

	pub fn group(&self) -> Option<&[u8]> {
		self.group.as_ref().map(|x| &x[..])
	}

	pub fn atime(&self) -> Option<i64> {
		self.atime
	}

	pub fn mtime(&self) -> Option<i64> {
		self.mtime
	}

	pub fn ctime(&self) -> Option<i64> {
		self.ctime
	}

	pub fn birthtime(&self) -> Option<i64> {
		self.birthtime
	}

	pub fn size(&self) -> Option<u64> {
		self.size
	}

	pub fn chunks(&self) -> &[Chunk] {
		self.chunks.as_ref().map(|x| &x[..]).unwrap_or(&[])
	}

	pub fn acl_access(&self) -> Option<Bytes> {
		self.acl_access.clone()
	}

	pub fn acl_default(&self) -> Option<Bytes> {
		self.acl_default.clone()
	}

	pub fn xattrs(&self) -> &HashMap<Bytes, Bytes> {
		&self.xattrs
	}

	pub fn into_xattrs(self) -> HashMap<Bytes, Bytes> {
		self.xattrs
	}

	pub fn source(&self) -> Option<Bytes> {
		self.source.clone()
	}
}
