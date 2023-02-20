use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::env::args;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::ops::Deref;
use std::os::unix::{ffi::OsStrExt, fs::symlink};
use std::path::MAIN_SEPARATOR;
use std::pin::Pin;
#[deny(unsafe_op_in_unsafe_fn)]
/*
things to display:

- local dsize: deduplicated size of the subtree within an archive
- local usage: size of chunks not used outside that subtree within an archive
- global dsize: deduplicated size of the subtree in the repository
- global usage: size of chunks not used outside that subtree in any archives
- churn: summed size of all chunks only used within that subtree *and* not in all versions
*/
use std::rc::Rc;
use std::sync::{atomic, atomic::AtomicBool, atomic::AtomicU64, Arc, Mutex, Weak};
use std::time::Instant;

use smartstring::alias::String as SmartString;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use cursive::align::HAlign;
use cursive::direction::Orientation;
use cursive::event::{Event, EventResult};
use cursive::theme::{BaseColor, Color, PaletteColor};
use cursive::traits::*;
use cursive::views::{Dialog, EditView, LinearLayout, OnEventView, Panel, ProgressBar, TextView};
use cursive::{Cursive, CursiveExt};

use cursive_table_view::{TableView, TableViewItem};

use ring::digest::{Context as DigestContext, SHA256};

use bytes::{BufMut, Bytes, BytesMut};

use anyhow::{Context, Result};

use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

use futures::stream::StreamExt;

use borgrdr::repository::Repository;
use borgrdr::rpc::RpcStoreClient;
use borgrdr::segments::Id;
use borgrdr::structs::{ArchiveItem, Chunk};

const CONTENT_HASH_LEN: usize = 32;
type ContentHash = Box<[u8; CONTENT_HASH_LEN]>;

static MERGED_NODES: AtomicU64 = AtomicU64::new(0);
static FINAL_NODES: AtomicU64 = AtomicU64::new(0);

fn hash_content(buf: &[u8]) -> ContentHash {
	let mut ctx = DigestContext::new(&SHA256);
	ctx.update(buf);
	let mut result = [0u8; 32];
	result.copy_from_slice(ctx.finish().as_ref());
	Box::new(result)
}

#[derive(Debug)]
enum FileData {
	Regular { chunks: Vec<Chunk>, size: u64 },
	Symlink { target_path: Bytes },
	Directory {},
}

#[derive(Debug)]
struct Times {
	atime: Option<DateTime<Utc>>,
	mtime: Option<DateTime<Utc>>,
	ctime: Option<DateTime<Utc>>,
	birthtime: Option<DateTime<Utc>>,
}

impl Default for Times {
	fn default() -> Self {
		Self {
			atime: None,
			mtime: None,
			ctime: None,
			birthtime: None,
		}
	}
}

fn convert_ts(ts: i64) -> DateTime<Utc> {
	let secs = ts / 1000000000;
	let nanos = (ts % 1000000000) as u32;
	Utc.timestamp(secs, nanos)
}

fn back_to_ts<'x>(v: &'x Result<DateTime<Utc>, String>) -> Cow<'x, str> {
	match v {
		Ok(v) => v.to_rfc3339().into(),
		Err(e) => e.as_str().into(),
	}
}

impl From<&ArchiveItem> for Times {
	fn from(other: &ArchiveItem) -> Self {
		Self {
			atime: other.atime().map(convert_ts),
			mtime: other.mtime().map(convert_ts),
			ctime: other.ctime().map(convert_ts),
			birthtime: other.birthtime().map(convert_ts),
		}
	}
}

struct ChunkSizes {
	original: u64,
	compressed: u64,
}

// chunkid -> osize, csize
type ChunkSizeMap = HashMap<Id, ChunkSizes>;

#[derive(Debug)]
struct VersionInfo {
	name: String,
	timestamp: Result<DateTime<Utc>, String>,
}

impl VersionInfo {
	fn timestamp_string(&self) -> String {
		match &self.timestamp {
			Ok(v) => v.format("%c").to_string(),
			Err(v) => v.clone(),
		}
	}
}

struct Version(Arc<VersionInfo>);

impl Deref for Version {
	type Target = VersionInfo;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for Version {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl PartialEq for Version {
	fn eq(&self, other: &Version) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}
}

impl Eq for Version {}

impl Hash for Version {
	fn hash<H: Hasher>(&self, state: &mut H) {
		Arc::as_ptr(&self.0).hash(state)
	}
}

impl fmt::Debug for Version {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		fmt::Debug::fmt(&*self.0, f)
	}
}

#[derive(Debug)]
struct Entity {
	numeric: u32,
	name: Option<SmartString>,
}

impl Entity {
	fn root() -> Self {
		Self {
			numeric: 0,
			name: Some("root".into()),
		}
	}

	fn from_bytes_lossy(name: Option<&[u8]>, numeric: u32) -> Self {
		Self {
			name: name.map(|x| String::from_utf8_lossy(x).into()),
			numeric,
		}
	}
}

#[derive(Debug)]
struct FileEntry {
	path: Vec<u8>,
	mode: u32,
	owner: Entity,
	group: Entity,
	times: Times,
	xattrs: Vec<(Vec<u8>, Vec<u8>)>,
	payload: FileData,
}

#[derive(Debug)]
enum FileEntryError {
	UnsupportedMode(u32),
}

impl FileEntry {
	fn split(self) -> (FileMetadata, FileData) {
		(
			FileMetadata {
				times: self.times,
				owner: self.owner,
				group: self.group,
				xattrs: self.xattrs,
				permissions: (self.mode & 0o7777) as u16,
			},
			self.payload,
		)
	}
}

impl TryFrom<ArchiveItem> for FileEntry {
	type Error = (FileEntryError, ArchiveItem);

	fn try_from(other: ArchiveItem) -> Result<Self, Self::Error> {
		let times = (&other).into();
		let mode = other.mode() & !0o7777;
		let payload = if mode == 0o40000 {
			// is directory
			FileData::Directory {}
		} else if mode == 0o120000 {
			// is symlink
			FileData::Symlink {
				target_path: other.source().expect("symlink without source!"),
			}
		} else if mode == 0o100000 {
			// is regular
			FileData::Regular {
				chunks: other.chunks().into_iter().cloned().collect(),
				size: other.size().unwrap_or(0),
			}
		} else {
			// unsupported
			return Err((FileEntryError::UnsupportedMode(mode), other));
		};
		let path = other.path().to_vec();
		let mode = other.mode();
		let owner = Entity::from_bytes_lossy(other.user(), other.uid());
		let group = Entity::from_bytes_lossy(other.group(), other.gid());
		let xattrs = other
			.into_xattrs()
			.into_iter()
			.map(|(k, v)| (k.into(), v.into()))
			.collect();
		Ok(Self {
			path,
			mode,
			owner,
			group,
			times,
			payload,
			xattrs,
		})
	}
}

async fn read_entries(
	repo: Repository<RpcStoreClient>,
	archive_sink: mpsc::Sender<(Version, mpsc::Receiver<FileEntry>)>,
	cb_sink: cursive::CbSink,
) -> anyhow::Result<Repository<RpcStoreClient>> {
	let manifest = repo.manifest();
	let narchives = manifest.archives().len();
	cb_sink.send(Box::new(move |siv: &mut Cursive| {
		siv.call_on_name("progress", |pb: &mut ProgressBar| {
			ArchiveProgress::ReadingArchives {
				done: 0,
				total: narchives,
			}
			.apply_to(pb)
		});
	}));
	let mut prepared_archives: Vec<(Id, borgrdr::structs::Archive)> = Vec::new();
	for (k, v) in manifest.archives().iter() {
		let archive = repo
			.read_archive(v.id())
			.await
			.with_context(|| format!("while reading archive info for {:?}", k))?;
		prepared_archives.push((*v.id(), archive));
	}

	let mut stream = repo
		.grouped_archive_items(
			prepared_archives
				.iter()
				.map(|(id, archive)| {
					(
						*id,
						archive
							.items()
							.iter()
							.map(|x| *x)
							.collect::<Vec<_>>()
							.into_iter(),
					)
				})
				.collect::<Vec<_>>()
				.into_iter(),
		)
		.await?;

	let mut i = 0;
	while let Some(id) = stream.identity() {
		assert!(*id == prepared_archives[i].0);
		let archive = &prepared_archives[i].1;
		let timestamp =
			match NaiveDateTime::parse_from_str(archive.start_time(), "%Y-%m-%dT%H:%M:%S%.6f") {
				Ok(v) => Ok(DateTime::from_utc(v, Utc)),
				Err(_) => Err(archive.start_time().into()),
			};
		let version = Version(Arc::new(VersionInfo {
			name: archive.name().into(),
			timestamp,
		}));
		let (item_sink, item_source) = mpsc::channel(128);
		archive_sink
			.send((version.clone(), item_source))
			.await
			.unwrap();
		while let Some(item) = stream.next().await {
			let item = item
				.with_context(|| format!("while reading item from archive {:?}", version.name))?;
			let item: FileEntry = match item.try_into() {
				Ok(v) => v,
				Err((e, item)) => {
					log::warn!("ignoring {:?}: {:?}", item.path(), e);
					continue;
				}
			};
			item_sink.send(item).await.unwrap();
		}
		Pin::new(&mut stream).next_group();
		i += 1;
		cb_sink.send(Box::new(move |siv: &mut Cursive| {
			siv.call_on_name("progress", |pb: &mut ProgressBar| {
				ArchiveProgress::ReadingArchives {
					done: i,
					total: narchives,
				}
				.apply_to(pb)
			});
		}));
	}
	drop(stream);

	cb_sink.send(Box::new(move |siv: &mut Cursive| {
		siv.call_on_name("progress", |pb: &mut ProgressBar| {
			ArchiveProgress::Done.apply_to(pb)
		});
	}));
	Ok(repo)
}

enum VersionedNode {
	Directory {
		children: HashMap<Vec<u8>, VersionedNode>,
		metadata: FileMetadata,
	},
	Regular {
		chunks: Vec<Id>,
		metadata: FileMetadata,
	},
	Symlink {
		target: Bytes,
		metadata: FileMetadata,
	},
}

fn split_first_segment<'x>(a: &mut Vec<u8>) -> Vec<u8> {
	for (i, b) in a.iter().enumerate() {
		if *b == b'/' {
			let lhs = a.drain(..i + 1).take(i).collect();
			return lhs;
		}
	}
	let mut result = Vec::new();
	std::mem::swap(&mut result, a);
	result
}

impl VersionedNode {
	pub fn new_directory(metadata: FileMetadata) -> Self {
		Self::Directory {
			children: HashMap::new(),
			metadata,
		}
	}

	pub fn from_file_entry(entry: FileEntry, chunk_index: &Arc<Mutex<ChunkSizeMap>>) -> Self {
		let (metadata, data) = entry.split();
		match data {
			FileData::Directory {} => Self::new_directory(metadata),
			FileData::Regular { chunks, .. } => {
				{
					let mut lock = chunk_index.lock().unwrap();
					for chunk in chunks.iter() {
						lock.insert(
							*chunk.id(),
							ChunkSizes {
								original: chunk.size(),
								compressed: chunk.csize(),
							},
						);
					}
				}
				Self::Regular {
					chunks: chunks.into_iter().map(|x| *x.id()).collect(),
					metadata,
				}
			}
			FileData::Symlink { target_path, .. } => Self::Symlink {
				target: target_path,
				metadata,
			},
		}
	}

	pub fn insert_node_at_path<'x>(
		&'x mut self,
		mut item: FileEntry,
		chunk_index: &'_ Arc<Mutex<ChunkSizeMap>>,
	) -> &'x mut VersionedNode {
		match self {
			Self::Directory { children, .. } => {
				let entry_name = split_first_segment(&mut item.path);
				let need_dir = item.path.len() > 0;
				let next = match children.entry(entry_name) {
					Entry::Vacant(v) => {
						if need_dir {
							v.insert(VersionedNode::new_directory(FileMetadata::default()))
						} else {
							return v.insert(Self::from_file_entry(item, chunk_index));
						}
					}
					Entry::Occupied(o) => o.into_mut(),
				};
				next.insert_node_at_path(item, chunk_index)
			}
			_ => panic!("node type conflict"),
		}
	}
}

impl fmt::Debug for VersionedNode {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Directory { children, metadata } => {
				write!(f, "Directory {{ metadata = {:?}, ", metadata)?;
				let mut debug = f.debug_map();
				for (item_path, item) in children.iter() {
					debug.entry(item_path, item);
				}
				debug.finish()?;
				f.write_str("}")?;
				Ok(())
			}
			Self::Regular { chunks, metadata } => {
				write!(f, "<{} chunks {:?}>", chunks.len(), metadata)
			}
			Self::Symlink { target, metadata } => {
				write!(f, "<symlink to {:?}, {:?}>", target, metadata)
			}
		}
	}
}

enum MergedNodeData {
	Directory {},
	Regular { chunks: Vec<Id> },
	Symlink { target: Bytes },
}

impl SizeEstimate for MergedNodeData {
	fn content_size(&self) -> usize {
		match self {
			Self::Regular { chunks } => chunks.capacity() * std::mem::size_of::<Id>(),
			_ => 0,
		}
	}
}

#[derive(Debug)]
struct FileMetadata {
	times: Times,
	permissions: u16,
	owner: Entity,
	group: Entity,
	xattrs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Default for FileMetadata {
	fn default() -> Self {
		Self {
			times: Times::default(),
			permissions: 0o0600,
			owner: Entity::root(),
			group: Entity::root(),
			xattrs: Vec::new(),
		}
	}
}

struct MergedNodeVersionGroup {
	data: MergedNodeData,
	// TODO: handle different metadata versions
	metadata: FileMetadata,
	sizes_done: AtomicBool,
	/// Original size of subtree
	osize: AtomicU64,
	/// Size after compression but before deduplication
	csize: AtomicU64,
	/// Accumulated size of chunks unique to this subtree and version group.
	group_dsize: AtomicU64,
	/// Accumulated size of chunks, deduplicated within this subtree and version group.
	local_dsize: AtomicU64,
	versions: Vec<Version>,
}

impl MergedNodeVersionGroup {
	fn calculate_original_sizes(
		subtree_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> (u64, u64) {
		let mut osize = 0;
		let mut csize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for (id, count) in subtree_chunks.iter() {
			let count = *count;
			let (chunk_osize, chunk_csize) = locked_chunk_index
				.get(id)
				.map(|sizes| (sizes.original, sizes.compressed))
				.unwrap_or((0, 0));
			osize += chunk_osize * count;
			csize += chunk_csize * count;
		}
		(osize, csize)
	}

	fn calculate_group_dsize(
		nversions: usize,
		subtree_chunks: &HashMap<Id, u64>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> u64 {
		let nversions = nversions as u64;
		let mut group_dsize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for (id, count) in subtree_chunks.iter() {
			let count = *count * nversions;
			if total_chunks.get(id).map(|x| *x).unwrap_or(0) > count {
				continue;
			}
			group_dsize += locked_chunk_index.get(id).unwrap().compressed;
		}
		group_dsize
	}

	fn calculate_local_dsize(
		subtree_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> u64 {
		let mut local_dsize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for id in subtree_chunks.keys() {
			local_dsize += locked_chunk_index.get(id).unwrap().compressed;
		}
		local_dsize
	}

	unsafe fn add_version(self: &Arc<Self>, version: Version) {
		assert_eq!(Arc::strong_count(self), 1);
		let this = unsafe { &mut *(Arc::as_ptr(self) as *mut Self) };
		this.versions.push(version);
	}

	fn calculate_sizes(
		self: &Arc<Self>,
		total_chunks: &HashMap<Id, u64>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) {
		let nversions = self.versions.len();
		let (osize, csize, group_dsize, local_dsize) =
			match self.versions.get(0).and_then(|x| subtree_chunks.get(x)) {
				Some(version_chunks) => {
					let (osize, csize) =
						Self::calculate_original_sizes(version_chunks, chunk_index);
					let group_dsize = Self::calculate_group_dsize(
						nversions,
						version_chunks,
						total_chunks,
						chunk_index,
					);
					let local_dsize = Self::calculate_local_dsize(version_chunks, chunk_index);
					(osize, csize, group_dsize, local_dsize)
				}
				None => (0, 0, 0, 0),
			};
		self.osize.store(osize, atomic::Ordering::Relaxed);
		self.csize.store(csize, atomic::Ordering::Relaxed);
		self.group_dsize
			.store(group_dsize, atomic::Ordering::Relaxed);
		self.local_dsize
			.store(local_dsize, atomic::Ordering::Relaxed);
		self.sizes_done.store(true, atomic::Ordering::Release);
	}
}

struct MergedNode {
	parent: Option<Weak<MergedNode>>,
	name: Vec<u8>,
	sizes_done: AtomicBool,
	/// Summed on-disk size of all chunks which appear *only* in this subtree,
	/// and only in one version of the subtree.
	churn: AtomicU64,
	/// Size of all chunks not used outside this subtree.
	usage: AtomicU64,
	/// Deduplicated size of this subtree across all version groups.
	local_dsize: AtomicU64,
	// for the record: changing this into a pair of HashMap<Vec<u8>, usize> and
	// Vec<Arc<MergedNode>> (with the hashmap pointing at the vec) and then
	// dropping the hashmap after building the tree worsens the memory use,
	// probably because of the fragmentation all those tiny hashmaps create.
	children: HashMap<Vec<u8>, Arc<MergedNode>>,
	version_groups: HashMap<ContentHash, Arc<MergedNodeVersionGroup>>,
}

fn f64_to_usize_checked(x: f64) -> Option<usize> {
	let x = x.round();
	if x > usize::MAX as f64 || x < usize::MIN as f64 {
		return None;
	}
	Some(x as usize)
}

impl MergedNode {
	fn new(parent: Weak<MergedNode>, name: Vec<u8>) -> Arc<Self> {
		MERGED_NODES.fetch_add(1, atomic::Ordering::Relaxed);
		Arc::new(Self {
			parent: Some(parent),
			name,
			sizes_done: AtomicBool::new(false),
			churn: AtomicU64::new(0),
			usage: AtomicU64::new(0),
			local_dsize: AtomicU64::new(0),
			children: HashMap::new(),
			version_groups: HashMap::new(),
		})
	}

	fn new_root() -> Arc<Self> {
		Arc::new(Self {
			parent: None,
			name: b"".to_vec(),
			sizes_done: AtomicBool::new(false),
			churn: AtomicU64::new(0),
			usage: AtomicU64::new(0),
			local_dsize: AtomicU64::new(0),
			children: HashMap::new(),
			version_groups: HashMap::new(),
		})
	}

	fn scale_capacity_estimate(max_chunks: usize, total_chunks: usize) -> usize {
		let estimated_chunks = ((max_chunks as f64) * 1.5).round();
		let estimated_chunks: usize =
			f64_to_usize_checked(estimated_chunks).unwrap_or(total_chunks);
		total_chunks.min(estimated_chunks)
	}

	fn capacity_with_headroom_for_union(
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		total_chunks: usize,
	) -> usize {
		Self::scale_capacity_estimate(
			subtree_chunks.values().map(|x| x.len()).max().unwrap_or(0),
			total_chunks,
		)
	}

	fn calculate_churn(
		versions: &HashMap<ContentHash, Arc<MergedNodeVersionGroup>>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> u64 {
		if versions.len() == 0 {
			return 0;
		}

		let locked_chunk_index = chunk_index.lock().unwrap();
		// count how many versions a chunk appears in
		let mut versions_union = HashMap::<Id, Option<u64>>::with_capacity(
			Self::capacity_with_headroom_for_union(subtree_chunks, locked_chunk_index.len()),
		);
		for version in versions.values() {
			let representative = match subtree_chunks.get(&version.versions[0]) {
				Some(v) => v,
				None => continue,
			};
			let nversions = version.versions.len() as u64;
			for (id, count) in representative.iter() {
				match versions_union.entry(*id) {
					Entry::Occupied(mut o) => {
						*o.get_mut() = None;
					}
					Entry::Vacant(v) => {
						let count = *count * nversions;
						v.insert(Some(count));
					}
				}
			}
		}

		let mut churned_size = 0;
		for (id, unique_count) in versions_union {
			let count = match unique_count {
				Some(v) => v,
				// chunk is not unique among versions
				None => continue,
			};
			if total_chunks.get(&id).map(|x| *x).unwrap_or(0) > count {
				// the chunk is used outside of this subtree
				continue;
			}
			// chunk is unique and not used outside -> add csize to churn
			churned_size += locked_chunk_index.get(&id).unwrap().compressed;
		}

		churned_size
	}

	fn calculate_usage(
		versions: &HashMap<ContentHash, Arc<MergedNodeVersionGroup>>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> u64 {
		if versions.len() == 0 {
			return 0;
		}

		let locked_chunk_index = chunk_index.lock().unwrap();
		// count total chunk occurences
		let mut versions_union = HashMap::with_capacity(Self::capacity_with_headroom_for_union(
			subtree_chunks,
			locked_chunk_index.len(),
		));

		for version in versions.values() {
			let representative = match subtree_chunks.get(&version.versions[0]) {
				Some(v) => v,
				None => continue,
			};
			let nversions = version.versions.len() as u64;
			for (id, count) in representative.iter() {
				let count = *count * nversions;
				match versions_union.entry(*id) {
					Entry::Occupied(mut o) => {
						*o.get_mut() += count;
					}
					Entry::Vacant(v) => {
						v.insert(count);
					}
				}
			}
		}

		let mut usage = 0;
		for (id, count) in versions_union {
			let global_count = total_chunks.get(&id).map(|x| *x).unwrap_or(0);
			debug_assert!(global_count >= count);
			if global_count > count {
				// the chunk is used outside of this subtree
				continue;
			}
			// chunk is unique and not used outside -> add csize to churn
			usage += locked_chunk_index.get(&id).unwrap().compressed;
		}

		usage
	}

	fn calculate_local_dsize(
		versions: &HashMap<ContentHash, Arc<MergedNodeVersionGroup>>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> u64 {
		let locked_chunk_index = chunk_index.lock().unwrap();
		let mut seen_chunks = HashSet::with_capacity(Self::capacity_with_headroom_for_union(
			subtree_chunks,
			locked_chunk_index.len(),
		));
		let mut dsize = 0;
		for version in versions.values() {
			let representative = match subtree_chunks.get(&version.versions[0]) {
				Some(v) => v,
				None => continue,
			};
			for id in representative.keys() {
				if seen_chunks.insert(*id) {
					dsize += locked_chunk_index.get(id).unwrap().compressed;
				}
			}
		}
		dsize
	}

	unsafe fn merge_v3(
		self: &Arc<MergedNode>,
		version: Version,
		node: VersionedNode,
	) -> ContentHash {
		assert_eq!(Arc::strong_count(self), 1);
		let this = unsafe { &mut *(Arc::as_ptr(self) as *mut Self) };
		let mut buf = BytesMut::new();
		let (data, metadata) = match node {
			VersionedNode::Directory { children, metadata } => {
				buf.put_u8(0x00);
				let mut sorted: Vec<_> = children.into_iter().collect();
				sorted.sort_by(|a, b| a.0.cmp(&b.0));
				for (name, new_child) in sorted {
					buf.reserve(8 + name.len() + CONTENT_HASH_LEN);
					buf.put_u64_le(name.len() as u64);
					buf.put_slice(&name[..]);
					let own_child = match this.children.entry(name) {
						Entry::Occupied(o) => o.into_mut(),
						Entry::Vacant(v) => {
							let name = v.key().clone();
							v.insert(Self::new(Arc::downgrade(self), name))
						}
					};
					let content_hash = unsafe { own_child.merge_v3(version.clone(), new_child) };
					buf.put_slice(&content_hash[..]);
				}
				(MergedNodeData::Directory {}, metadata)
			}
			VersionedNode::Regular { chunks, metadata } => {
				buf.put_u8(0x01);
				buf.reserve(chunks.len() * 32 + 1);
				for chunk in chunks.iter() {
					buf.put_slice(&chunk.0[..]);
				}
				(MergedNodeData::Regular { chunks }, metadata)
			}
			VersionedNode::Symlink { target, metadata } => {
				buf.reserve(2 + target.len());
				buf.put_u8(0x02);
				buf.put_u8(0x00);
				buf.put_slice(&target[..]);
				(MergedNodeData::Symlink { target }, metadata)
			}
		};
		let content_hash = hash_content(&buf);
		match this.version_groups.entry(content_hash.clone()) {
			Entry::Occupied(mut o) => unsafe {
				o.get_mut().add_version(version.clone());
			},
			Entry::Vacant(v) => {
				v.insert(Arc::new(MergedNodeVersionGroup {
					data,
					metadata,
					sizes_done: AtomicBool::new(false),
					osize: AtomicU64::new(0),
					csize: AtomicU64::new(0),
					group_dsize: AtomicU64::new(0),
					local_dsize: AtomicU64::new(0),
					versions: vec![version.clone()],
				}));
			}
		};
		content_hash
	}

	fn calculate_sizes_inner(
		self: &Arc<Self>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkSizeMap>>,
	) -> HashMap<Version, HashMap<Id, u64>> {
		let mut subtree_chunks = HashMap::new();
		for (_, child) in self.children.iter() {
			let mut child_chunks = child.calculate_sizes_inner(total_chunks, chunk_index);
			// we always merge into the larger one
			if subtree_chunks.len() < child_chunks.len() {
				std::mem::swap(&mut subtree_chunks, &mut child_chunks);
			}

			for (version_key, mut version_child_chunks) in child_chunks {
				match subtree_chunks.entry(version_key) {
					Entry::Vacant(v) => {
						// not in there yet, just move child data over.
						v.insert(version_child_chunks);
						continue;
					}
					Entry::Occupied(mut o) => {
						let version_subtree_chunks = o.get_mut();
						// we always merge into the larger one
						if version_subtree_chunks.len() < version_child_chunks.len() {
							std::mem::swap(version_subtree_chunks, &mut version_child_chunks);
						}
						for (id, count) in version_child_chunks {
							match version_subtree_chunks.entry(id) {
								Entry::Occupied(mut o) => {
									*o.get_mut() += count;
								}
								Entry::Vacant(v) => {
									v.insert(count);
								}
							}
						}
					}
				}
			}
		}

		// now we have the accurate subtree_chunks, now we need to add any chunks from *this* object itself in order to include it in calculations
		for version_group in self.version_groups.values() {
			match &version_group.data {
				MergedNodeData::Regular { chunks } => {
					for version in version_group.versions.iter() {
						let dest = match subtree_chunks.entry(version.clone()) {
							Entry::Vacant(v) => v.insert(HashMap::new()),
							Entry::Occupied(o) => o.into_mut(),
						};
						for id in chunks.iter() {
							match dest.entry(*id) {
								Entry::Vacant(v) => {
									v.insert(1);
								}
								Entry::Occupied(mut o) => {
									*o.get_mut() += 1;
								}
							}
						}
					}
				}
				_ => (),
			}
		}

		for (_, version_group) in self.version_groups.iter() {
			version_group.calculate_sizes(total_chunks, &subtree_chunks, chunk_index);
		}

		let churn = Self::calculate_churn(
			&self.version_groups,
			&subtree_chunks,
			total_chunks,
			chunk_index,
		);
		let local_dsize =
			Self::calculate_local_dsize(&self.version_groups, &subtree_chunks, chunk_index);
		let usage = Self::calculate_usage(
			&self.version_groups,
			&subtree_chunks,
			total_chunks,
			chunk_index,
		);

		self.churn.store(churn, atomic::Ordering::Relaxed);
		self.local_dsize
			.store(local_dsize, atomic::Ordering::Relaxed);
		self.usage.store(usage, atomic::Ordering::Relaxed);

		FINAL_NODES.fetch_add(1, atomic::Ordering::Relaxed);
		self.sizes_done.store(true, atomic::Ordering::Release);

		subtree_chunks
	}

	fn gather_chunk_maps(&self, total: &mut HashMap<Id, u64>) {
		for version in self.version_groups.values() {
			match &version.data {
				MergedNodeData::Regular { chunks } => {
					let count = version.versions.len() as u64;
					for id in chunks.iter() {
						match total.entry(*id) {
							Entry::Occupied(mut o) => {
								*o.get_mut() += count;
							}
							Entry::Vacant(v) => {
								v.insert(count);
							}
						}
					}
				}
				_ => (),
			}
		}
		for child in self.children.values() {
			child.gather_chunk_maps(total)
		}
	}

	fn global_chunk_map(&self, chunk_estimate: usize) -> HashMap<Id, u64> {
		let mut total_chunks = HashMap::with_capacity(chunk_estimate);
		self.gather_chunk_maps(&mut total_chunks);
		total_chunks
	}

	fn version_group_of(&self, version: &Version) -> Option<&MergedNodeVersionGroup> {
		for group in self.version_groups.values() {
			if group.versions.iter().any(|x| x == version) {
				return Some(group);
			}
		}
		None
	}
}

async fn hasher(
	mut src: mpsc::Receiver<(Version, mpsc::Receiver<FileEntry>)>,
	dst: mpsc::Sender<(Version, VersionedNode)>,
	chunk_index: Arc<Mutex<ChunkSizeMap>>,
) {
	while let Some((version, mut item_source)) = src.recv().await {
		log::info!("processing archive {:?}", version);
		let mut root = VersionedNode::new_directory(FileMetadata::default());
		while let Some(item) = item_source.recv().await {
			root.insert_node_at_path(item, &chunk_index);
		}
		dst.send((version, root)).await.unwrap();
	}
}

type GroupKey = Bytes;

trait SizeEstimate: Sized {
	fn content_size(&self) -> usize;

	fn recursive_size(&self) -> usize {
		self.content_size() + std::mem::size_of::<Self>()
	}
}

fn format_bytes(n: u64) -> String {
	/* let order_of_magnitude = (64 - n.leading_zeros()) / 10;
	let (suffix, divisor): (_, u64) = match order_of_magnitude {
		// bytes
		0 => ("B  ", 1),
		1 => ("kiB", 1024),
		2 => ("MiB", 1024*1024),
		3 => ("GiB", 1024*1024*1024),
		4 => ("TiB", 1024*1024*1024*1024),
		_ => ("EiB", 1024*1024*1024*1024*1024),
	};
	let value = ((n as f64) / (divisor as f64));
	format!("{:3.0} {}", value, suffix) */
	let order_of_magnitude = if n > 0 {
		(n as f64).log(1024.).floor() as u64
	} else {
		0
	};
	let (suffix, divisor): (_, u64) = match order_of_magnitude {
		0 => (" ", 1),
		1 => ("k", 1024),
		2 => ("M", 1024 * 1024),
		3 => ("G", 1024 * 1024 * 1024),
		4 => ("T", 1024 * 1024 * 1024 * 1024),
		_ => ("E", 1024 * 1024 * 1024 * 1024 * 1024),
	};
	let value = (n as f64) / (divisor as f64);
	let ndigits = if value != 0. {
		value.log10().floor() as u64
	} else {
		3
	};
	match ndigits {
		0 => format!("{:>4.2}{}", value, suffix),
		1 => format!("{:>4.1}{}", value, suffix),
		_ => format!("{:>4.0}{}", value, suffix),
	}
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum VersionColumn {
	Name,
	OriginalSize,
	GroupDsize,
	LocalDsize,
}

#[derive(Clone)]
enum VersionItem {
	VersionGroup {
		group: Arc<MergedNodeVersionGroup>,
	},
	Version {
		version: Version,
		group: Arc<MergedNodeVersionGroup>,
	},
}

impl VersionItem {
	fn osize(&self) -> Option<u64> {
		let group = self.group();
		if !group.sizes_done.load(atomic::Ordering::Acquire) {
			return None;
		}
		Some(group.osize.load(atomic::Ordering::Relaxed))
	}

	fn group_dsize(&self) -> Option<u64> {
		let group = self.group();
		if !group.sizes_done.load(atomic::Ordering::Acquire) {
			return None;
		}
		Some(group.group_dsize.load(atomic::Ordering::Relaxed))
	}

	fn local_dsize(&self) -> Option<u64> {
		let group = self.group();
		if !group.sizes_done.load(atomic::Ordering::Acquire) {
			return None;
		}
		Some(group.local_dsize.load(atomic::Ordering::Relaxed))
	}

	fn group(&self) -> &Arc<MergedNodeVersionGroup> {
		match self {
			Self::VersionGroup { group, .. } => group,
			Self::Version { group, .. } => group,
		}
	}

	fn cmp_group(
		a: &Arc<MergedNodeVersionGroup>,
		b: &Arc<MergedNodeVersionGroup>,
		column: VersionColumn,
	) -> Ordering {
		let order = match column {
			VersionColumn::Name => {
				let a_ts = a.versions.iter().map(|x| back_to_ts(&x.timestamp)).min();
				let b_ts = b.versions.iter().map(|x| back_to_ts(&x.timestamp)).min();
				a_ts.cmp(&b_ts)
			}
			VersionColumn::OriginalSize => a
				.osize
				.load(atomic::Ordering::Relaxed)
				.cmp(&b.osize.load(atomic::Ordering::Relaxed)),
			VersionColumn::GroupDsize => a
				.group_dsize
				.load(atomic::Ordering::Relaxed)
				.cmp(&b.group_dsize.load(atomic::Ordering::Relaxed)),
			VersionColumn::LocalDsize => a
				.local_dsize
				.load(atomic::Ordering::Relaxed)
				.cmp(&b.local_dsize.load(atomic::Ordering::Relaxed)),
		};
		if order == Ordering::Equal {
			// use memory address as tie breaker
			return (Arc::as_ptr(a) as usize).cmp(&(Arc::as_ptr(b) as usize));
		}
		order
	}
}

impl TableViewItem<VersionColumn> for VersionItem {
	fn to_column(&self, column: VersionColumn) -> String {
		let size = match column {
			VersionColumn::Name => match self {
				Self::VersionGroup { group, .. } => {
					let earliest = group
						.versions
						.iter()
						.min_by_key(|version| &version.timestamp)
						.unwrap()
						.timestamp_string();
					if group.versions.len() > 1 {
						return format!("{} (+{})", earliest, group.versions.len() - 1);
					} else {
						return earliest;
					}
				}
				Self::Version { version, .. } => {
					return format!("| {}", version.timestamp_string())
				}
			},
			VersionColumn::OriginalSize => self.osize(),
			VersionColumn::GroupDsize => self.group_dsize(),
			VersionColumn::LocalDsize => self.local_dsize(),
		};
		match size {
			Some(size) => format_bytes(size),
			None => "??".into(),
		}
	}

	fn cmp(&self, other: &Self, column: VersionColumn) -> Ordering {
		let this_group = self.group();
		let other_group = other.group();
		// if the groups differ, we order based on the group
		if !Arc::ptr_eq(this_group, other_group) {
			// use the group comparison function
			return Self::cmp_group(this_group, other_group, column);
		}

		// dig deeper
		match self {
			// if this is a version group, we order before the other, because we are the same group and want to be in front of our versions
			Self::VersionGroup { .. } => Ordering::Less,
			Self::Version {
				version: this_version,
				..
			} => match other {
				// vice versa
				Self::VersionGroup { .. } => Ordering::Greater,
				Self::Version {
					version: other_version,
					..
				} => match column {
					VersionColumn::Name
					| VersionColumn::OriginalSize
					| VersionColumn::GroupDsize
					| VersionColumn::LocalDsize => this_version.0.name.cmp(&other_version.0.name),
				},
			},
		}
	}
}

async fn prepare(
	url: String,
	cb_sink: cursive::CbSink,
) -> Result<
	(
		Repository<RpcStoreClient>,
		Arc<MergedNode>,
		Arc<Mutex<ChunkSizeMap>>,
	),
	anyhow::Error,
> {
	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&url)
		.await
		.with_context(|| format!("failed to open repository"))?;

	let (entry_sink, archive_source) = mpsc::channel(128);
	let (hashed_sink, mut hashed_source) = mpsc::channel(2);
	let chunk_index = Arc::new(Mutex::new(ChunkSizeMap::new()));
	let reader = tokio::spawn(read_entries(repo, entry_sink, cb_sink));
	let hasher = tokio::spawn(hasher(
		archive_source,
		hashed_sink,
		Arc::clone(&chunk_index),
	));

	let merged = MergedNode::new_root();
	while let Some((version, hashed_tree)) = hashed_source.recv().await {
		// merged has not been shared between threads yet, so this is safe.
		unsafe {
			merged.merge_v3(version, hashed_tree);
		}
	}

	hasher.await.unwrap();
	let repo = reader.await.unwrap()?;

	Ok((repo, merged, chunk_index))
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum FileListColumn {
	Name,
	Versions,
	MaxOSize,
	Churn,
	DSize,
	Usage,
}

impl MergedNode {
	fn max_osize(&self) -> Option<u64> {
		self.version_groups
			.values()
			.filter_map(|x| {
				if x.sizes_done.load(atomic::Ordering::Acquire) {
					Some(x.osize.load(atomic::Ordering::Relaxed))
				} else {
					None
				}
			})
			.max()
	}

	fn distinct_versions(&self) -> u64 {
		self.version_groups.len() as u64
	}

	fn total_versions(&self) -> u64 {
		self.version_groups
			.values()
			.map(|x| x.versions.len() as u64)
			.sum()
	}
}

impl TableViewItem<FileListColumn> for Arc<MergedNode> {
	fn to_column(&self, column: FileListColumn) -> String {
		let sizes_done = self.sizes_done.load(atomic::Ordering::Acquire);
		let size = match column {
			FileListColumn::Name => {
				let mut name = String::from_utf8_lossy(&self.name).to_string();
				if self.children.len() > 0 {
					name.push_str("/")
				}
				return name;
			}
			FileListColumn::Versions => {
				return format!("{}/{}", self.distinct_versions(), self.total_versions());
			}
			FileListColumn::MaxOSize => {
				return match self.max_osize() {
					Some(v) => format_bytes(v),
					None => "??".into(),
				}
			}
			FileListColumn::Churn => self.churn.load(atomic::Ordering::Relaxed),
			FileListColumn::DSize => self.local_dsize.load(atomic::Ordering::Relaxed),
			FileListColumn::Usage => self.usage.load(atomic::Ordering::Relaxed),
		};
		if !sizes_done {
			return "??".into();
		}
		format_bytes(size)
	}

	fn cmp(&self, other: &Self, column: FileListColumn) -> Ordering {
		match column {
			FileListColumn::Versions => self.distinct_versions().cmp(&other.distinct_versions()),
			FileListColumn::MaxOSize => self.max_osize().cmp(&other.max_osize()),
			FileListColumn::Churn => self
				.churn
				.load(atomic::Ordering::Relaxed)
				.cmp(&other.churn.load(atomic::Ordering::Relaxed)),
			FileListColumn::DSize => self
				.local_dsize
				.load(atomic::Ordering::Relaxed)
				.cmp(&other.local_dsize.load(atomic::Ordering::Relaxed)),
			FileListColumn::Usage => self
				.usage
				.load(atomic::Ordering::Relaxed)
				.cmp(&other.usage.load(atomic::Ordering::Relaxed)),
			_ => self.to_column(column).cmp(&other.to_column(column)),
		}
	}
}

enum ArchiveProgress {
	Opening,
	ReadingArchives { done: usize, total: usize },
	Done,
}

impl ArchiveProgress {
	fn apply_to(self, pb: &mut ProgressBar) {
		match self {
			Self::Opening => {
				pb.set_label(|_, _| "opening...".to_string());
				pb.set_range(0, 1);
				pb.set_value(0);
			}
			Self::ReadingArchives { done, total } => {
				pb.set_label(|value, (min, max)| {
					format!(
						"{:.0}%",
						(value - min) as f64 / ((max - min).max(1) as f64) * 100.
					)
				});
				pb.set_range(0, total);
				pb.set_value(done);
			}
			Self::Done => {
				pb.set_range(0, 1);
				pb.set_value(1);
			}
		}
	}

	fn applied(self, mut pb: ProgressBar) -> ProgressBar {
		self.apply_to(&mut pb);
		pb
	}
}

fn apply_finalisation_progress(pb: &mut ProgressBar) {
	let merged = MERGED_NODES.load(atomic::Ordering::Relaxed);
	let finalized = FINAL_NODES.load(atomic::Ordering::Relaxed);
	// some headroom :-)
	pb.set_range(0, (merged + 2) as usize);
	pb.set_value(finalized.min(merged) as usize);
}

struct Main {
	root: Arc<MergedNode>,
	current_parent: Arc<MergedNode>,
}

impl Main {
	fn update_root(this: Arc<Mutex<Main>>, root: Arc<MergedNode>) {
		let mut this = this.lock().unwrap();
		this.root = Arc::clone(&root);
		this.current_parent = root;
	}
}

struct ExtractProgress {
	cancel_flag: AtomicBool,
	extracted_nodes: AtomicU64,
	extracted_bytes: AtomicU64,
}

#[async_recursion::async_recursion]
async fn extract(
	repo: &Repository<RpcStoreClient>,
	version: &Version,
	subtree: Arc<MergedNode>,
	dest_path: &OsString,
	progress: &ExtractProgress,
) -> Result<bool, anyhow::Error> {
	let v = match subtree.version_group_of(&version) {
		None => {
			// there is nothing here in this version, skip
			return Ok(true);
		}
		Some(v) => v,
	};
	if subtree.name == b".." || subtree.name == b"." {
		// we refuse to extract these!
		// .. silently ..
		return Ok(true);
	}
	let name = OsStr::from_bytes(&subtree.name);
	let mut path = dest_path.clone();
	// TODO: use MAIN_SEPARATOR_STR once it stabilizes
	let mut buf = String::new();
	buf.push(MAIN_SEPARATOR);
	path.push(&buf);
	path.push(&name);
	match &v.data {
		MergedNodeData::Directory {} => {
			// directory, we need to mkdir, and then recurse
			std::fs::create_dir(&path)
				.with_context(|| format!("creating output directory: {:?}", path))?;
			for (_, node) in subtree.children.iter() {
				if !extract(repo, version, Arc::clone(node), &path, progress).await? {
					// cancelled
					return Ok(false);
				}
			}
			// TODO: extract metadata
		}
		MergedNodeData::Regular { chunks, .. } => {
			// file, we need to extract
			let mut out = std::fs::File::create(&path)?;
			if progress.cancel_flag.load(atomic::Ordering::Relaxed) {
				return Ok(false);
			}
			let mut reader = repo
				.open_stream(chunks.clone())
				.with_context(|| format!("opening source stream for {:?}", path))?;
			let mut buffer = Box::new([0u8; 8192]);
			loop {
				if progress.cancel_flag.load(atomic::Ordering::Relaxed) {
					return Ok(false);
				}
				let data = match reader
					.read(&mut buffer[..])
					.await
					.with_context(|| format!("reading source data for {:?}", path))?
				{
					0 => break,
					n => &buffer[..n],
				};
				progress
					.extracted_bytes
					.fetch_add(data.len() as u64, atomic::Ordering::Relaxed);
				out.write_all(data)
					.with_context(|| format!("writing data to {:?}", path))?;
			}
			// TODO: extract metadata
		}
		MergedNodeData::Symlink { target, .. } => {
			// symlink, need to create that
			let target = OsString::from(OsStr::from_bytes(&target));
			symlink(&target, &path)
				.with_context(|| format!("creating symlink to {:?} at {:?}", target, path))?;
		}
	};
	progress
		.extracted_nodes
		.fetch_add(1, atomic::Ordering::Relaxed);
	if progress.cancel_flag.load(atomic::Ordering::Relaxed) {
		return Ok(false);
	}
	Ok(true)
}

enum WorkerCommand {
	Extract {
		dest_path: String,
		subtree: Arc<MergedNode>,
		version: Version,
		progress: Arc<ExtractProgress>,
		reply: Box<dyn FnOnce(Result<bool, anyhow::Error>) + Send + 'static>,
	},
}

fn safescale(v: u64, max: u64) -> usize {
	assert!(v <= max);
	if usize::BITS >= u64::BITS {
		return v as usize;
	}
	let mut shift = 0;
	while (max >> shift) > usize::MAX as u64 {
		shift += 8;
	}
	(v >> shift) as usize
}

fn fill_versions(table: &mut TableView<VersionItem, VersionColumn>, item: &Arc<MergedNode>) {
	let total_versions: usize = item.version_groups.values().map(|x| x.versions.len()).sum();
	let mut items = Vec::with_capacity(item.version_groups.len() + total_versions);
	for version_group in item.version_groups.values() {
		items.push(VersionItem::VersionGroup {
			group: Arc::clone(version_group),
		});
	}
	let has_any = items.len() > 0;
	table.set_items(items);
	if has_any {
		table.set_selected_row(0);
	}
}

fn apply_extraction_progress(siv: &mut Cursive, progress: &ExtractProgress, osize: Option<u64>) {
	let bytes = progress.extracted_bytes.load(atomic::Ordering::Relaxed);
	siv.call_on_name("extracted_nodes", |tv: &mut TextView| {
		tv.set_content(
			progress
				.extracted_nodes
				.load(atomic::Ordering::Relaxed)
				.to_string(),
		);
	});
	siv.call_on_name("extracted_bytes", |tv: &mut TextView| {
		tv.set_content(format_bytes(bytes));
	});
	if let Some(osize) = osize {
		siv.call_on_name("extract_progress", |pb: &mut ProgressBar| {
			pb.set_value(safescale(bytes, osize));
		});
	}
}

fn main() -> Result<(), anyhow::Error> {
	env_logger::init();
	let mut argv: Vec<String> = args().collect();

	let empty_root = MergedNode::new_root();
	let main = Arc::new(Mutex::new(Main {
		root: Arc::clone(&empty_root),
		current_parent: Arc::clone(&empty_root),
	}));

	let mut siv = cursive::Cursive::default();

	let sender = siv.cb_sink().clone();
	let (cmdtx, mut cmdrx) = mpsc::unbounded_channel();
	{
		let main = Arc::clone(&main);
		std::thread::spawn(move || {
			let t0 = Instant::now();
			let rt = tokio::runtime::Runtime::new().unwrap();
			rt.block_on(async move {
				let (repo, data, chunk_index) = match prepare(argv.remove(1), sender.clone()).await
				{
					Ok(v) => v,
					Err(e) => {
						let msg = format!("{:#}", e);
						sender
							.send(Box::new(|siv: &mut Cursive| {
								siv.add_layer(Dialog::text(msg).button("Quit", |siv| siv.quit()))
							}))
							.unwrap();
						return;
					}
				};
				std::thread::spawn(move || {
					let total_chunk_count = {
						let lock = chunk_index.lock().unwrap();
						lock.len()
					};
					Main::update_root(Arc::clone(&main), Arc::clone(&data));
					let t1 = Instant::now();
					let duration = t1.duration_since(t0);
					{
						let data = Arc::clone(&data);
						sender.send(Box::new(move |siv: &mut Cursive| {
							let item = siv
								.call_on_name(
									"contents",
									|table: &mut TableView<Arc<MergedNode>, FileListColumn>| {
										table.set_items(data.children.values().cloned().collect());
										table.set_selected_row(0);
										table
											.borrow_item(table.item().unwrap_or(0))
											.map(|x| Arc::clone(x))
									},
								)
								.unwrap();
							if let Some(item) = item {
								siv.call_on_name(
									"versions",
									move |table: &mut TableView<VersionItem, VersionColumn>| {
										fill_versions(table, &item);
									},
								);
							}
							siv.call_on_name("statusbar_text", |tv: &mut TextView| {
								let seconds = duration.as_secs_f64();
								let minutes = (seconds / 60.0).floor();
								let seconds = seconds - minutes * 60.0;
								tv.set_content(format!(
									"Repository read in {:.0}min {:.2}s",
									minutes, seconds
								));
							});
							siv.pop_layer();
							siv.set_fps(1);
							siv.add_global_callback(Event::Refresh, |siv| {
								siv.call_on_name("statusbar_progress", |pb: &mut ProgressBar| {
									apply_finalisation_progress(pb);
								});
							});
						}));
					}
					let total_chunks = data.global_chunk_map(total_chunk_count);
					data.calculate_sizes_inner(&total_chunks, &chunk_index);
					drop(total_chunks);
					drop(chunk_index);
					sender.send(Box::new(move |siv: &mut Cursive| {
						siv.set_fps(0);
						siv.clear_global_callbacks(Event::Refresh);
						siv.call_on_name("statusbar_progress", |pb: &mut ProgressBar| {
							// ensure this shows 100% when done :-)
							pb.set_range(0, 1);
							pb.set_value(1);
						});
					}));
				});
				loop {
					let item = match cmdrx.recv().await {
						Some(v) => v,
						None => break,
					};
					match item {
						WorkerCommand::Extract {
							version,
							dest_path,
							subtree,
							progress,
							reply,
						} => {
							let path = dest_path.into();
							reply(extract(&repo, &version, subtree, &path, &progress).await);
						}
					}
				}
			});
			rt.shutdown_background();
		});
	}

	let mut table = TableView::<Arc<MergedNode>, FileListColumn>::new()
		.column(FileListColumn::Name, "Name", |c| c)
		.column(FileListColumn::Versions, "#V", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(FileListColumn::MaxOSize, "O<", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(FileListColumn::DSize, "D", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(FileListColumn::Churn, "Ch", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(FileListColumn::Usage, "Us", |c| {
			c.width(6).align(HAlign::Right)
		});

	{
		let main = Arc::clone(&main);
		table.set_on_submit(move |siv: &mut Cursive, _row: usize, index: usize| {
			let main = Arc::clone(&main);
			let item = siv
				.call_on_name(
					"contents",
					|table: &mut TableView<Arc<MergedNode>, FileListColumn>| {
						let item = table.borrow_item(index).unwrap();
						if item.children.len() > 0 {
							{
								let mut lock = main.lock().unwrap();
								lock.current_parent = Arc::clone(&item);
							}
							let items: Vec<_> = item.children.values().cloned().collect();
							drop(item);
							let has_any = items.len() > 0;
							table.set_items(items);
							if has_any {
								table.set_selected_row(0);
								table
									.borrow_item(table.item().unwrap())
									.map(|x| Arc::clone(x))
							} else {
								None
							}
						} else {
							None
						}
					},
				)
				.unwrap();
			if let Some(item) = item {
				siv.call_on_name(
					"versions",
					move |table: &mut TableView<VersionItem, VersionColumn>| {
						fill_versions(table, &item);
					},
				);
			}
		});
	}
	table.set_on_select(move |siv: &mut Cursive, row: usize, index: usize| {
		let item = siv
			.call_on_name(
				"contents",
				move |table: &mut TableView<Arc<MergedNode>, FileListColumn>| {
					Arc::clone(&table.borrow_item(index).unwrap())
				},
			)
			.unwrap();
		siv.call_on_name(
			"versions",
			move |table: &mut TableView<VersionItem, VersionColumn>| {
				fill_versions(table, &item);
			},
		);
	});
	let mut table = OnEventView::new(table.with_name("contents"));
	table.set_on_event('x', |siv| {
		siv.add_layer(Dialog::info(
			"Select a version from the version table to extract",
		));
	});

	/* siv.update_theme(|th| {
		th.palette[PaletteColor::View] = Color::Dark(BaseColor::Black);
		th.palette[PaletteColor::Primary] = Color::Dark(BaseColor::White);
		th.palette[PaletteColor::TitlePrimary] = Color::Light(BaseColor::Cyan);
		th.palette[PaletteColor::Highlight] = Color::Light(BaseColor::Cyan);
		th.palette[PaletteColor::HighlightInactive] = Color::Dark(BaseColor::Blue);
		th.palette[PaletteColor::HighlightText] = Color::Dark(BaseColor::Black);
	}); */
	siv.add_global_callback('q', |s| s.quit());

	siv.add_global_callback(cursive::event::Key::F1, |siv| {
		siv.add_layer(
			Dialog::new()
				.title("Help")
				.content(TextView::new(cursive::utils::markup::markdown::parse(
					"**Columns**

**#V**  number of unique versions and number of total versions
**O<**  maximum original size across versions
**O**   original size in this subtree version
**D**   deduplicated size of all versions together
**Ch**  churn: sum of chunks of data only used in this subtree and not used in all versions
**Us**  usage: sum of chunks only used in this subtree
**gD**  sum of chunks unique to this subtree version
**lD**  deduplicated size of this subtree version
        (ignoring deduplication with data outside this subtree)
",
				)))
				.button("Ok", |siv| {
					siv.pop_layer();
				}),
		);
	});

	{
		let main = Arc::clone(&main);
		siv.add_global_callback(cursive::event::Key::Backspace, move |siv| {
			let main = Arc::clone(&main);
			let item = siv
				.call_on_name(
					"contents",
					move |table: &mut TableView<Arc<MergedNode>, FileListColumn>| {
						let mut lock = main.lock().unwrap();
						let old_parent = Arc::clone(&lock.current_parent);
						if let Some(parent) = old_parent.parent.as_ref().and_then(|x| x.upgrade()) {
							let items: Vec<_> = parent.children.values().cloned().collect();
							let selected_index = items
								.iter()
								.enumerate()
								.find(|(_, x)| Arc::ptr_eq(&old_parent, x))
								.map(|(i, _)| i)
								.unwrap_or(0);
							table.set_items(items);
							lock.current_parent = Arc::clone(&parent);
							table.set_selected_item(selected_index);
							Some(parent)
						} else {
							None
						}
					},
				)
				.unwrap();
			if let Some(item) = item {
				siv.call_on_name(
					"versions",
					move |table: &mut TableView<VersionItem, VersionColumn>| {
						fill_versions(table, &item);
					},
				);
			}
		});
	}

	let mut version_table = TableView::<VersionItem, VersionColumn>::new()
		.column(VersionColumn::Name, "Name", |c| c)
		.column(VersionColumn::OriginalSize, "O", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(VersionColumn::GroupDsize, "gD", |c| {
			c.width(6).align(HAlign::Right)
		})
		.column(VersionColumn::LocalDsize, "lD", |c| {
			c.width(6).align(HAlign::Right)
		});
	version_table.set_on_submit(move |siv: &mut Cursive, _row: usize, index: usize| {
		let group = siv
			.call_on_name(
				"versions",
				|table: &mut TableView<VersionItem, VersionColumn>| {
					Arc::clone(table.borrow_item(index).unwrap().group())
				},
			)
			.unwrap();
		let subtree = siv
			.call_on_name(
				"contents",
				move |table: &mut TableView<Arc<MergedNode>, FileListColumn>| {
					Arc::clone(&table.borrow_item(table.item().unwrap()).unwrap())
				},
			)
			.unwrap();
		let primary_version = &group.versions.get(0).unwrap().name;

		let subtree = Arc::clone(&subtree);
		let cmdtx = cmdtx.clone();
		let version = group.versions[0].clone();
		let group = Arc::clone(&group);
		let extract = move |siv: &mut Cursive, s: &str| {
			let progress = Arc::new(ExtractProgress {
				cancel_flag: AtomicBool::new(false),
				extracted_nodes: AtomicU64::new(0),
				extracted_bytes: AtomicU64::new(0),
			});
			siv.pop_layer();
			let osize = if group.sizes_done.load(atomic::Ordering::Acquire) {
				Some(group.osize.load(atomic::Ordering::Relaxed))
			} else {
				None
			};
			let mut progress_layout = LinearLayout::vertical()
				.child(
					LinearLayout::horizontal()
						.child(TextView::new("Items:"))
						.child(
							TextView::new("0")
								.h_align(HAlign::Right)
								.with_name("extracted_nodes")
								.fixed_width(20),
						),
				)
				.child(
					LinearLayout::horizontal()
						.child(TextView::new("Size: "))
						.child(
							TextView::new("0")
								.h_align(HAlign::Right)
								.with_name("extracted_bytes")
								.fixed_width(20),
						),
				);
			if let Some(osize) = osize {
				progress_layout.add_child(
					ProgressBar::new()
						.min(0)
						.max(safescale(osize, osize))
						.with_name("extract_progress"),
				);
			}
			let progress_dialog = {
				let progress = Arc::clone(&progress);
				Dialog::new()
					.title("Extracting...")
					.content(progress_layout)
					.button("Cancel", move |_| {
						progress.cancel_flag.store(true, atomic::Ordering::Relaxed)
					})
					.with_name("extract_dialog")
			};
			siv.add_layer(progress_dialog);
			siv.set_fps(1);
			{
				let progress = Arc::clone(&progress);
				siv.add_global_callback(Event::Refresh, move |siv| {
					apply_extraction_progress(siv, &progress, osize);
				});
			}

			// now we actually have to EXTRACT things *gasp*
			let ch = siv.cb_sink().clone();
			cmdtx.send(WorkerCommand::Extract {
				dest_path: s.into(),
				version: version.clone(),
				subtree: Arc::clone(&subtree),
				progress: Arc::clone(&progress),
				reply: Box::new(move |result| {
					ch.send(Box::new(move |siv: &mut Cursive| {
						siv.set_fps(0);
						siv.clear_global_callbacks(Event::Refresh);
						match result {
							Ok(_) => {
								apply_extraction_progress(siv, &progress, osize);
								siv.call_on_name("extract_dialog", |d: &mut Dialog| {
									d.clear_buttons();
									d.add_button("Close", |siv| {
										siv.pop_layer();
									});
								});
							}
							Err(e) => {
								siv.pop_layer();
								siv.add_layer(
									Dialog::new()
										.title("Extraction failed")
										.content(TextView::new(e.to_string()))
										.dismiss_button("Close"),
								);
							}
						}
					}));
				}),
			});
		};

		let try_extract =
			move |siv: &mut Cursive, s: Rc<String>| match std::fs::metadata(s.as_str()) {
				Ok(metadata) if metadata.is_dir() => {
					extract(siv, s.as_str());
				}
				Ok(_) => {
					siv.add_layer(
						Dialog::new()
							.title("Error")
							.content(TextView::new("Output path exists, but is not a directory"))
							.dismiss_button("Ok"),
					);
				}
				Err(e) if e.kind() == io::ErrorKind::NotFound => {
					let extract = extract.clone();
					siv.add_layer(
						Dialog::new()
							.title("Create output directory?")
							.content(TextView::new("Output directory does not exist.\nCreate?"))
							.button("No", |siv| {
								siv.pop_layer();
							})
							.button("Yes", move |siv| match std::fs::create_dir(s.as_str()) {
								Ok(_) => {
									siv.pop_layer();
									extract(siv, s.as_str());
								}
								Err(e) => {
									siv.pop_layer();
									siv.add_layer(
										Dialog::new()
											.title("Error")
											.content(TextView::new(e.to_string()))
											.dismiss_button("Ok"),
									);
								}
							}),
					);
				}
				Err(e) => {
					siv.add_layer(
						Dialog::new()
							.title("Error")
							.content(TextView::new("Cannot access output path"))
							.dismiss_button("Ok"),
					);
				}
			};

		let destination_picker = {
			let try_extract_on_submit = try_extract.clone();
			Dialog::new()
				.title("Extract files")
				.content(
					LinearLayout::vertical()
						.child(TextView::new("Extract to"))
						.child(
							EditView::new()
								.content(format!("./{}", primary_version))
								.on_submit(move |siv, s| {
									try_extract_on_submit(siv, Rc::new(s.to_string()))
								})
								.with_name("extract_to"),
						),
				)
				.button("Ok", move |siv| {
					let s = siv
						.call_on_name("extract_to", |edit: &mut EditView| edit.get_content())
						.unwrap();
					try_extract(siv, s);
				})
				.dismiss_button("Cancel")
		};
		siv.add_layer(destination_picker);
	});
	let version_table = OnEventView::new(version_table.with_name("versions"));

	siv.add_fullscreen_layer(
		LinearLayout::new(Orientation::Vertical)
			.child(Panel::new(table).title("Repository contents").full_height())
			.child(
				Panel::new(version_table)
					.title("File versions")
					.full_height(),
			)
			.child(
				LinearLayout::new(Orientation::Horizontal)
					.child(
						TextView::new("Ready to rumble.")
							.with_name("statusbar_text")
							.full_width(),
					)
					.child(
						ProgressBar::new()
							.with_name("statusbar_progress")
							.min_width(16),
					)
					.with_name("statusbar")
					.fixed_height(1),
			)
			.full_screen(),
	);

	siv.add_layer(
		Dialog::new()
			.title("Loading")
			.content(
				LinearLayout::new(Orientation::Vertical)
					.child(
						TextView::new("Reading archives")
							.with_name("progress_step")
							.fixed_height(1),
					)
					.child(
						ArchiveProgress::Opening
							.applied(ProgressBar::new())
							.with_name("progress")
							.min_width(16),
					),
			)
			.button("Cancel", |siv| {
				siv.quit();
			}),
	);

	siv.run();
	Ok(())
}
