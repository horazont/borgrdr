use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::path::PathBuf;

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

#[derive(Deserialize, Debug)]
pub struct Archive {
	version: u8,
	name: String,
	cmdline: Vec<String>,
	hostname: String,
	username: String,
	time: String,
	time_end: String,
	comment: String,
	chunker_params: (String, usize, usize, usize, usize),
	items: Vec<Id>,
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

	pub fn chunker_params(&self) -> (&str, usize, usize, usize, usize) {
		(
			&self.chunker_params.0,
			self.chunker_params.1,
			self.chunker_params.2,
			self.chunker_params.3,
			self.chunker_params.4,
		)
	}

	pub fn comment(&self) -> &str {
		&self.comment
	}

	pub fn items(&self) -> &[Id] {
		&self.items
	}
}

#[derive(Deserialize, Debug)]
pub struct Chunk(Id, u64, u64);

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

// TODOC: names!
#[derive(Deserialize, Debug)]
pub struct ArchiveItem {
	path: Bytes,
	mode: u32,
	uid: u32,
	gid: u32,
	user: Bytes,
	group: Bytes,
	atime: Option<i64>,
	ctime: Option<i64>,
	mtime: Option<i64>,
	birthtime: Option<i64>,
	size: Option<u64>,
	chunks: Option<Vec<Chunk>>,
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

	pub fn user(&self) -> &[u8] {
		&self.user
	}

	pub fn group(&self) -> &[u8] {
		&self.group
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
}
