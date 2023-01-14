/*
things to display:

- local dsize: deduplicated size of the subtree within an archive
- local usage: size of chunks not used outside that subtree within an archive
- global dsize: deduplicated size of the subtree in the repository
- global usage: size of chunks not used outside that subtree in any archives
- churn: summed size of all chunks only used within that subtree *and* not in all versions
*/

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::env::args;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use cursive::align::HAlign;
use cursive::direction::Orientation;
use cursive::theme::{BaseColor, Color, PaletteColor};
use cursive::traits::*;
use cursive::views::{Dialog, LinearLayout, Panel, ProgressBar};
use cursive::{Cursive, CursiveExt};

use cursive_tree_view::{Placement, TreeView};

use cursive_table_view::{TableView, TableViewItem};

use ring::digest::{Context as DigestContext, SHA256};

use bytes::{BufMut, Bytes, BytesMut};

use anyhow::{Context, Result};

use tokio::sync::mpsc;

use futures::stream::StreamExt;

use chrono::{DateTime, TimeZone, Utc};

use borgrdr::repository::Repository;
use borgrdr::rpc::RpcStoreClient;
use borgrdr::segments::Id;
use borgrdr::structs::{ArchiveItem, Chunk};

fn hash_content(buf: &[u8]) -> Bytes {
	let mut ctx = DigestContext::new(&SHA256);
	ctx.update(buf);
	Bytes::copy_from_slice(ctx.finish().as_ref())
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

fn convert_ts(ts: i64) -> DateTime<Utc> {
	let secs = ts / 1000000000;
	let nanos = (ts % 1000000000) as u32;
	Utc.timestamp(secs, nanos)
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

// chunkid -> osize, csize
type ChunkIndex = HashMap<Id, (u64, u64)>;

#[derive(Debug)]
struct VersionInfo {
	name: String,
	// TODO: convert to DateTime<Utc>
	timestamp: String,
}

struct Version(Arc<VersionInfo>);

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
struct FileEntry {
	path: Bytes,
	mode: u32,
	uid: u32,
	gid: u32,
	times: Times,
	payload: FileData,
}

#[derive(Debug)]
enum FileEntryError {
	UnsupportedMode(u32),
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
				target_path: Bytes::new(),
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
		Ok(Self {
			path: other.path().clone(),
			mode: other.mode(),
			uid: other.uid(),
			gid: other.gid(),
			times,
			payload,
		})
	}
}

async fn read_entries(
	repo: Repository<RpcStoreClient>,
	archive_sink: mpsc::Sender<(Version, mpsc::Receiver<FileEntry>)>,
	cb_sink: cursive::CbSink,
) -> Result<(), io::Error> {
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
	for (i, v) in manifest.archives().values().enumerate() {
		let (version, items) = {
			let archive = repo.read_archive(v.id()).await?;
			let version = Version(Arc::new(VersionInfo {
				name: archive.name().into(),
				timestamp: archive.start_time().into(),
			}));
			(version, archive.items().iter().map(|x| *x).collect())
		};
		let (item_sink, item_source) = mpsc::channel(128);
		archive_sink.send((version, item_source)).await.unwrap();
		let mut archive_item_stream = repo.archive_items(items)?;
		while let Some(item) = archive_item_stream.next().await {
			let item = item?;
			let item: FileEntry = match item.try_into() {
				Ok(v) => v,
				Err((e, item)) => {
					log::warn!("ignoring {:?}: {:?}", item.path(), e);
					continue;
				}
			};
			item_sink.send(item).await.unwrap();
		}
		cb_sink.send(Box::new(move |siv: &mut Cursive| {
			siv.call_on_name("progress", |pb: &mut ProgressBar| {
				ArchiveProgress::ReadingArchives {
					done: i + 1,
					total: narchives,
				}
				.apply_to(pb)
			});
		}));
	}
	cb_sink.send(Box::new(move |siv: &mut Cursive| {
		siv.call_on_name("progress", |pb: &mut ProgressBar| {
			ArchiveProgress::Done.apply_to(pb)
		});
	}));
	Ok(())
}

enum VersionedNode {
	Directory {
		children: HashMap<Bytes, VersionedNode>,
	},
	Regular {
		chunks: Vec<Id>,
	},
	Symlink {
		target: Bytes,
	},
}

fn split_path<'x>(a: &'x [u8]) -> (&'x [u8], Option<&'x [u8]>) {
	for (i, b) in a.iter().enumerate() {
		if *b == b'/' {
			return (&a[..i], Some(&a[i + 1..]));
		}
	}
	return (a, None);
}

fn has_dir_sep(a: &[u8]) -> bool {
	return a.iter().find(|x| **x == b'/').is_some();
}

fn split_first_segment<'x>(a: &mut Bytes) -> Bytes {
	for (i, b) in a.iter().enumerate() {
		if *b == b'/' {
			let mut lhs = a.split_to(i + 1);
			lhs.truncate(i);
			return lhs;
		}
	}
	let mut result = Bytes::new();
	std::mem::swap(&mut result, a);
	result
}

impl VersionedNode {
	pub fn new_directory() -> Self {
		Self::Directory {
			children: HashMap::new(),
		}
	}

	pub fn from_file_entry(entry: FileEntry, chunk_index: &Arc<Mutex<ChunkIndex>>) -> Self {
		match entry.payload {
			FileData::Directory {} => Self::new_directory(),
			FileData::Regular { chunks, .. } => {
				{
					let mut lock = chunk_index.lock().unwrap();
					for chunk in chunks.iter() {
						lock.insert(*chunk.id(), (chunk.size(), chunk.csize()));
					}
				}
				Self::Regular {
					chunks: chunks.into_iter().map(|x| *x.id()).collect(),
				}
			}
			FileData::Symlink { target_path, .. } => Self::Symlink {
				target: target_path,
			},
		}
	}

	pub fn insert_node_at_path<'x>(
		&'x mut self,
		mut item: FileEntry,
		chunk_index: &'_ Arc<Mutex<ChunkIndex>>,
	) -> &'x mut VersionedNode {
		match self {
			Self::Directory { children } => {
				let entry_name = split_first_segment(&mut item.path);
				let need_dir = item.path.len() > 0;
				let next = match children.entry(entry_name) {
					Entry::Vacant(v) => {
						if need_dir {
							v.insert(VersionedNode::new_directory())
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
			Self::Directory { children } => {
				let mut debug = f.debug_map();
				for (item_path, item) in children.iter() {
					debug.entry(item_path, item);
				}
				debug.finish()
			}
			Self::Regular { chunks } => {
				write!(f, "<{} chunks>", chunks.len())
			}
			Self::Symlink { target } => {
				write!(f, "<symlink to {:?}>", target)
			}
		}
	}
}

enum HashedNodeData {
	Directory {
		children: HashMap<Bytes, HashedNode>,
	},
	Regular {
		chunks: Vec<Id>,
	},
	Symlink {
		target: Bytes,
	},
}

impl HashedNodeData {
	fn content_hash(&self) -> Bytes {
		let mut buf = BytesMut::new();
		match self {
			Self::Directory { children } => {
				buf.put_u8(0x00);
				let mut sorted: Vec<_> = children.iter().collect();
				sorted.sort_by(|a, b| a.0.cmp(b.0));
				for (name, child) in sorted {
					buf.reserve(16 + name.len() + child.content_hash.len());
					buf.put_u64_le(name.len() as u64);
					buf.put_slice(&name[..]);
					buf.put_u64_le(child.content_hash.len() as u64);
					buf.put_slice(&child.content_hash[..]);
				}
			}
			Self::Regular { chunks } => {
				buf.put_u8(0x01);
				buf.reserve(chunks.len() * 32 + 1);
				for chunk in chunks.iter() {
					buf.put_slice(&chunk.0[..]);
				}
			}
			Self::Symlink { target } => {
				buf.reserve(2 + target.len());
				buf.put_u8(0x02);
				buf.put_u8(0x00);
				buf.put_slice(&target[..]);
			}
		}
		hash_content(&buf)
	}

	fn split_for_merge(self) -> (MergedNodeData, Option<HashMap<Bytes, HashedNode>>) {
		match self {
			Self::Directory { children } => (MergedNodeData::Directory {}, Some(children)),
			Self::Regular { chunks } => (MergedNodeData::Regular { chunks }, None),
			Self::Symlink { target } => (MergedNodeData::Symlink { target }, None),
		}
	}
}

impl HashedNodeData {
	fn from_versioned(other: VersionedNode, chunk_index: &Arc<Mutex<ChunkIndex>>) -> Self {
		match other {
			VersionedNode::Directory { children } => Self::Directory {
				children: children
					.into_iter()
					.map(|(path, node)| (path, HashedNode::from_versioned(node, chunk_index)))
					.collect(),
			},
			VersionedNode::Regular { chunks } => Self::Regular { chunks },
			VersionedNode::Symlink { target } => Self::Symlink { target },
		}
	}
}

#[derive(Debug)]
struct HashedNode {
	content_hash: Bytes,
	data: HashedNodeData,
}

impl HashedNode {
	pub fn from_versioned(other: VersionedNode, chunk_index: &Arc<Mutex<ChunkIndex>>) -> Self {
		let data = HashedNodeData::from_versioned(other, chunk_index);
		Self {
			content_hash: data.content_hash(),
			data,
		}
	}
}

impl fmt::Debug for HashedNodeData {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Directory { children } => {
				let mut debug = f.debug_map();
				for (item_path, item) in children.iter() {
					debug.entry(item_path, item);
				}
				debug.finish()
			}
			Self::Regular { chunks } => {
				write!(f, "<{} chunks>", chunks.len())
			}
			Self::Symlink { target } => {
				write!(f, "<symlink to {:?}>", target)
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

struct MergedNodeVersion {
	data: MergedNodeData,
	versions: Vec<Version>,
}

struct MergedNode {
	name: Bytes,
	children: HashMap<Bytes, MergedNode>,
	versions: HashMap<Bytes, MergedNodeVersion>,
}

static INDENT: &'static str = "│ ";
static INDENT_LAST: &'static str = "  ";

impl MergedNode {
	fn new(name: Bytes) -> Self {
		Self {
			name,
			children: HashMap::new(),
			versions: HashMap::new(),
		}
	}

	fn new_root() -> Self {
		Self::new((&b"/"[..]).into())
	}

	fn merge(&mut self, version: Version, node: HashedNode) {
		let children = match self.versions.entry(node.content_hash) {
			Entry::Occupied(mut o) => {
				o.get_mut().versions.push(version.clone());
				let (_, children) = node.data.split_for_merge();
				children
			}
			Entry::Vacant(v) => {
				let (data, children) = node.data.split_for_merge();
				v.insert(MergedNodeVersion {
					data,
					versions: vec![version.clone()],
				});
				children
			}
		};
		if let Some(children) = children {
			for (path, new_child) in children {
				let mut own_child = match self.children.entry(path) {
					Entry::Occupied(o) => o.into_mut(),
					Entry::Vacant(v) => {
						let name = v.key().clone();
						v.insert(Self::new(name))
					}
				};
				own_child.merge(version.clone(), new_child);
			}
		}
	}

	fn gather_chunk_maps(&self, total: &mut HashMap<Id, u64>) {
		for version in self.versions.values() {
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
}

async fn hasher(
	mut src: mpsc::Receiver<(Version, mpsc::Receiver<FileEntry>)>,
	dst: mpsc::Sender<(Version, HashedNode)>,
	chunk_index: Arc<Mutex<ChunkIndex>>,
) {
	while let Some((version, mut item_source)) = src.recv().await {
		log::info!("processing archive {:?}", version);
		let mut root = VersionedNode::new_directory();
		while let Some(item) = item_source.recv().await {
			root.insert_node_at_path(item, &chunk_index);
		}
		let root = HashedNode::from_versioned(root, &chunk_index);
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

struct FinalNodeVersion {
	version: Version,
}

impl SizeEstimate for FinalNodeVersion {
	fn content_size(&self) -> usize {
		0
	}
}

impl FinalNodeVersion {
	fn from_merged(other: Version) -> Arc<Self> {
		Arc::new(Self { version: other })
	}
}

struct FinalNodeVersionGroup {
	/// Origina-l size of subtree
	osize: u64,
	/// Size after compression but before deduplication
	csize: u64,
	/// Accumulated size of chunks unique to this subtree and version group.
	group_dsize: u64,
	/// Accumulated size of chunks, deduplicated within this subtree and version group.
	local_dsize: u64,
	data: MergedNodeData,
	versions: Vec<Arc<FinalNodeVersion>>,
}

impl SizeEstimate for FinalNodeVersionGroup {
	fn content_size(&self) -> usize {
		let versions_size: usize = self.versions.iter().map(|x| x.recursive_size()).sum();
		self.versions.capacity() * std::mem::size_of::<Arc<FinalNodeVersion>>()
			+ versions_size
			+ self.data.content_size()
	}
}

impl FinalNodeVersionGroup {
	fn calculate_original_sizes(
		subtree_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> (u64, u64) {
		let mut osize = 0;
		let mut csize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for (id, count) in subtree_chunks.iter() {
			let count = *count;
			let (chunk_osize, chunk_csize) = locked_chunk_index
				.get(id)
				.map(|(osize, csize)| (*osize, *csize))
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
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> u64 {
		let nversions = nversions as u64;
		let mut group_dsize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for (id, count) in subtree_chunks.iter() {
			let count = *count * nversions;
			if total_chunks.get(id).map(|x| *x).unwrap_or(0) > count {
				continue;
			}
			group_dsize += locked_chunk_index.get(id).unwrap().1;
		}
		group_dsize
	}

	fn calculate_local_dsize(
		subtree_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> u64 {
		let mut local_dsize = 0;
		let locked_chunk_index = chunk_index.lock().unwrap();
		for id in subtree_chunks.keys() {
			local_dsize += locked_chunk_index.get(id).unwrap().1;
		}
		local_dsize
	}

	fn from_merged(
		other: MergedNodeVersion,
		total_chunks: &HashMap<Id, u64>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> Arc<Self> {
		let nversions = other.versions.len();
		let (osize, csize, group_dsize, local_dsize) =
			match other.versions.get(0).and_then(|x| subtree_chunks.get(x)) {
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
		let versions = other
			.versions
			.into_iter()
			.map(|x| FinalNodeVersion::from_merged(x))
			.collect();
		Arc::new(Self {
			data: other.data,
			versions,
			osize,
			csize,
			group_dsize,
			local_dsize,
		})
	}
}

struct FinalNode {
	parent: Option<Weak<FinalNode>>,
	name: Bytes,
	/// Summed on-disk size of all chunks which appear *only* in this subtree,
	/// and only in one version of the subtree.
	churn: u64,
	/// Size of all chunks not used outside this subtree.
	usage: u64,
	/// Deduplicated size of this subtree across all version groups.
	local_dsize: u64,
	version_groups: Vec<Arc<FinalNodeVersionGroup>>,
	children: Vec<Arc<FinalNode>>,
}

impl SizeEstimate for FinalNode {
	fn content_size(&self) -> usize {
		let children_size: usize = self.children.iter().map(|x| x.recursive_size()).sum();
		let version_groups_size: usize =
			self.version_groups.iter().map(|x| x.recursive_size()).sum();
		self.version_groups.capacity() * std::mem::size_of::<Arc<FinalNodeVersionGroup>>()
			+ self.children.capacity() * std::mem::size_of::<Arc<FinalNode>>()
			+ version_groups_size
			+ children_size
	}
}

fn min_max<I: Iterator<Item = usize>>(i: I) -> Option<(usize, usize)> {
	let mut result: Option<(usize, usize)> = None;
	for item in i {
		match result.as_mut() {
			Some((min, max)) => {
				*min = (*min).min(item);
				*max = (*max).max(item);
			}
			None => {
				result = Some((item, item));
			}
		}
	}
	result
}

impl FinalNode {
	fn new_empty() -> Arc<Self> {
		Arc::new(Self {
			parent: None,
			name: Bytes::from_static(b""),
			churn: 0,
			usage: 0,
			local_dsize: 0,
			version_groups: Vec::new(),
			children: Vec::new(),
		})
	}

	fn scale_capacity_estimate(max_chunks: usize, total_chunks: usize) -> usize {
		let estimated_chunks = ((max_chunks as f64) * 1.5).round();
		let estimated_chunks: usize = match max_chunks.try_into() {
			Ok(v) => v,
			Err(_) => total_chunks,
		};
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
		versions: &HashMap<Bytes, MergedNodeVersion>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
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
			churned_size += locked_chunk_index.get(&id).unwrap().1;
		}

		churned_size
	}

	fn calculate_usage(
		versions: &HashMap<Bytes, MergedNodeVersion>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
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
			usage += locked_chunk_index.get(&id).unwrap().1;
		}

		usage
	}

	fn calculate_local_dsize(
		versions: &HashMap<Bytes, MergedNodeVersion>,
		subtree_chunks: &HashMap<Version, HashMap<Id, u64>>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
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
					dsize += locked_chunk_index.get(id).unwrap().1;
				}
			}
		}
		dsize
	}

	fn from_merged_inner(
		parent: Option<Weak<FinalNode>>,
		other: MergedNode,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> (Arc<Self>, HashMap<Version, HashMap<Id, u64>>) {
		let mut subtree_chunks = HashMap::new();
		let this = Arc::new_cyclic(|this| {
			let mut children = Vec::with_capacity(other.children.len());
			for (name, child) in other.children {
				let (child, mut child_chunks) = Self::from_merged_inner(
					Some(Weak::clone(this)),
					child,
					total_chunks,
					chunk_index,
				);
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
				children.push(child);
			}

			// now we have the accurate subtree_chunks, now we need to add any chunks from *this* object itself in order to include it in calculations
			for version_group in other.versions.values() {
				match &version_group.data {
					MergedNodeData::Regular { chunks } => {
						for version in version_group.versions.iter() {
							let dest = match subtree_chunks.entry(version.clone()) {
								Entry::Vacant(v) => v.insert(HashMap::new()),
								Entry::Occupied(mut o) => o.into_mut(),
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

			let churn =
				Self::calculate_churn(&other.versions, &subtree_chunks, total_chunks, chunk_index);
			let local_dsize =
				Self::calculate_local_dsize(&other.versions, &subtree_chunks, chunk_index);
			let usage =
				Self::calculate_usage(&other.versions, &subtree_chunks, total_chunks, chunk_index);

			let mut version_groups = Vec::with_capacity(other.versions.len());
			for (version_key, version_group) in other.versions {
				version_groups.push(FinalNodeVersionGroup::from_merged(
					version_group,
					total_chunks,
					&subtree_chunks,
					chunk_index,
				));
			}

			Self {
				name: other.name,
				parent,
				churn,
				usage,
				local_dsize,
				version_groups,
				children,
			}
		});
		(this, subtree_chunks)
	}

	pub fn from_merged(
		parent: Option<Weak<FinalNode>>,
		other: MergedNode,
		total_chunks: &HashMap<Id, u64>,
		chunk_index: &Arc<Mutex<ChunkIndex>>,
	) -> Arc<Self> {
		Self::from_merged_inner(parent, other, total_chunks, chunk_index).0
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
		(n as f64).log10().floor() as u64
	} else {
		0
	};
	let (suffix, divisor): (_, u64) = match order_of_magnitude / 3 {
		0 => ("B  ", 1),
		1 => ("kiB", 1024),
		2 => ("MiB", 1024 * 1024),
		3 => ("GiB", 1024 * 1024 * 1024),
		4 => ("TiB", 1024 * 1024 * 1024 * 1024),
		_ => ("EiB", 1024 * 1024 * 1024 * 1024 * 1024),
	};
	let value = ((n as f64) / (divisor as f64));
	if order_of_magnitude >= 3 {
		match order_of_magnitude % 3 {
			0 => format!("{:3.2} {}", value, suffix),
			_ => format!("{:3.0} {}", value, suffix),
		}
	} else {
		format!("{:3.0} {}", value, suffix)
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
		group: Arc<FinalNodeVersionGroup>,
	},
	Version {
		version: Arc<FinalNodeVersion>,
		group: Arc<FinalNodeVersionGroup>,
	},
}

impl VersionItem {
	fn osize(&self) -> u64 {
		match self {
			Self::VersionGroup { group, .. } => group.osize,
			Self::Version { group, .. } => group.osize,
		}
	}

	fn group(&self) -> &Arc<FinalNodeVersionGroup> {
		match self {
			Self::VersionGroup { group, .. } => group,
			Self::Version { group, .. } => group,
		}
	}

	fn cmp_group(
		a: &Arc<FinalNodeVersionGroup>,
		b: &Arc<FinalNodeVersionGroup>,
		column: VersionColumn,
	) -> Ordering {
		let order = match column {
			VersionColumn::Name => a.versions.len().cmp(&b.versions.len()),
			VersionColumn::OriginalSize => a.osize.cmp(&b.osize),
			VersionColumn::GroupDsize => a.group_dsize.cmp(&b.group_dsize),
			VersionColumn::LocalDsize => a.local_dsize.cmp(&b.local_dsize),
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
					return format!("({} identical versions)", group.versions.len())
				}
				Self::Version { version, .. } => return format!("| {}", version.version.0.name),
			},
			VersionColumn::OriginalSize => self.osize(),
			VersionColumn::GroupDsize => self.group().group_dsize,
			VersionColumn::LocalDsize => self.group().local_dsize,
		};
		format_bytes(size)
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
					| VersionColumn::LocalDsize => this_version
						.version
						.0
						.name
						.cmp(&other_version.version.0.name),
				},
			},
		}
	}
}

async fn prepare(
	url: String,
	cb_sink: cursive::CbSink,
) -> Result<(MergedNode, Arc<Mutex<ChunkIndex>>), anyhow::Error> {
	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&url)
		.await
		.with_context(|| format!("failed to open repository"))?;

	let (entry_sink, archive_source) = mpsc::channel(128);
	let (hashed_sink, mut hashed_source) = mpsc::channel(2);
	let chunk_index = Arc::new(Mutex::new(ChunkIndex::new()));
	let reader = tokio::spawn(read_entries(repo, entry_sink, cb_sink));
	let hasher = tokio::spawn(hasher(
		archive_source,
		hashed_sink,
		Arc::clone(&chunk_index),
	));

	let mut merged = MergedNode::new_root();
	while let Some((version, hashed_tree)) = hashed_source.recv().await {
		merged.merge(version, hashed_tree);
	}

	hasher.await.unwrap();
	reader.await.unwrap()?;
	let _: Result<_, _> = backend.wait_for_shutdown().await;

	Ok((merged, chunk_index))
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

impl FinalNode {
	fn max_osize(&self) -> Option<u64> {
		self.version_groups.iter().map(|x| x.osize).max()
	}

	fn distinct_versions(&self) -> u64 {
		self.version_groups.len() as u64
	}

	fn total_versions(&self) -> u64 {
		self.version_groups
			.iter()
			.map(|x| x.versions.len() as u64)
			.sum()
	}
}

impl TableViewItem<FileListColumn> for Arc<FinalNode> {
	fn to_column(&self, column: FileListColumn) -> String {
		match column {
			FileListColumn::Name => {
				let mut name = String::from_utf8_lossy(&self.name).to_string();
				if self.children.len() > 0 {
					name.push_str("/")
				}
				name
			}
			FileListColumn::Versions => {
				format!("{}/{}", self.distinct_versions(), self.total_versions())
			}
			FileListColumn::MaxOSize => match self.max_osize() {
				Some(v) => format_bytes(v),
				None => "".into(),
			},
			FileListColumn::Churn => format_bytes(self.churn),
			FileListColumn::DSize => format_bytes(self.local_dsize),
			FileListColumn::Usage => format_bytes(self.usage),
		}
	}

	fn cmp(&self, other: &Self, column: FileListColumn) -> Ordering {
		match column {
			FileListColumn::Versions => self.distinct_versions().cmp(&other.distinct_versions()),
			FileListColumn::MaxOSize => self.max_osize().cmp(&other.max_osize()),
			FileListColumn::Churn => self.churn.cmp(&other.churn),
			FileListColumn::DSize => self.local_dsize.cmp(&other.local_dsize),
			FileListColumn::Usage => self.usage.cmp(&other.usage),
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
				pb.set_label(|_, _| "finalizing...".to_string());
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

struct Main {
	root: Arc<FinalNode>,
	current_parent: Arc<FinalNode>,
}

impl Main {
	fn update_root(this: Arc<Mutex<Main>>, root: Arc<FinalNode>) {
		let mut this = this.lock().unwrap();
		this.root = Arc::clone(&root);
		this.current_parent = root;
	}
}

fn main() -> Result<(), anyhow::Error> {
	env_logger::init();
	let mut argv: Vec<String> = args().collect();

	let empty_root = FinalNode::new_empty();
	let main = Arc::new(Mutex::new(Main {
		root: Arc::clone(&empty_root),
		current_parent: Arc::clone(&empty_root),
	}));

	let mut siv = cursive::Cursive::default();

	let sender = siv.cb_sink().clone();

	{
		let main = Arc::clone(&main);
		std::thread::spawn(move || {
			let rt = tokio::runtime::Runtime::new().unwrap();
			let (mut merged, chunk_index) =
				match rt.block_on(prepare(argv.remove(1), sender.clone())) {
					Ok(v) => v,
					Err(e) => {
						let msg = e.to_string();
						sender
							.send(Box::new(|siv: &mut Cursive| {
								siv.add_layer(Dialog::text(msg).button("Quit", |siv| siv.quit()))
							}))
							.unwrap();
						return;
					}
				};
			rt.shutdown_background();
			let total_chunk_count = {
				let lock = chunk_index.lock().unwrap();
				lock.len()
			};
			let total_chunks = merged.global_chunk_map(total_chunk_count);
			let data = FinalNode::from_merged(None, merged, &total_chunks, &chunk_index);
			drop(chunk_index);
			drop(total_chunks);
			Main::update_root(Arc::clone(&main), Arc::clone(&data));
			sender.send(Box::new(move |siv: &mut Cursive| {
				siv.call_on_name(
					"contents",
					|table: &mut TableView<Arc<FinalNode>, FileListColumn>| {
						table.set_items(data.children.clone());
						table.set_selected_row(0);
					},
				);
				siv.pop_layer();
			}));
		});
	}

	let mut table = TableView::<Arc<FinalNode>, FileListColumn>::new()
		.column(FileListColumn::Name, "Name", |c| c)
		.column(FileListColumn::Versions, "#V", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(FileListColumn::MaxOSize, "OSz<", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(FileListColumn::DSize, "DSz", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(FileListColumn::Churn, "Chrn", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(FileListColumn::Usage, "Usge", |c| {
			c.width(8).align(HAlign::Right)
		});

	{
		let main = Arc::clone(&main);
		table.set_on_submit(move |siv: &mut Cursive, row: usize, index: usize| {
			let main = Arc::clone(&main);
			siv.call_on_name(
				"contents",
				|table: &mut TableView<Arc<FinalNode>, FileListColumn>| {
					let item = table.borrow_item(index).unwrap();
					if item.children.len() > 0 {
						{
							let mut lock = main.lock().unwrap();
							lock.current_parent = Arc::clone(&item);
						}
						let items: Vec<_> = item.children.clone();
						drop(item);
						let has_any = items.len() > 0;
						table.set_items(items);
						if has_any {
							table.set_selected_row(0);
						}
					}
				},
			);
		});
	}
	table.set_on_select(move |siv: &mut Cursive, row: usize, index: usize| {
		let item = siv
			.call_on_name(
				"contents",
				move |table: &mut TableView<Arc<FinalNode>, FileListColumn>| {
					Arc::clone(&table.borrow_item(index).unwrap())
				},
			)
			.unwrap();
		siv.call_on_name(
			"versions",
			move |table: &mut TableView<VersionItem, VersionColumn>| {
				let total_versions: usize =
					item.version_groups.iter().map(|x| x.versions.len()).sum();
				let mut items = Vec::with_capacity(item.version_groups.len() + total_versions);
				for version_group in item.version_groups.iter() {
					items.push(VersionItem::VersionGroup {
						group: Arc::clone(version_group),
					});
					for version in version_group.versions.iter() {
						items.push(VersionItem::Version {
							group: Arc::clone(version_group),
							version: Arc::clone(version),
						});
					}
				}
				let has_any = items.len() > 0;
				table.set_items(items);
				if has_any {
					table.set_selected_row(0);
				}
			},
		);
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
	{
		let main = Arc::clone(&main);
		siv.add_global_callback(cursive::event::Key::Backspace, move |s| {
			let main = Arc::clone(&main);
			s.call_on_name(
				"contents",
				move |table: &mut TableView<Arc<FinalNode>, FileListColumn>| {
					let mut lock = main.lock().unwrap();
					let old_parent = Arc::clone(&lock.current_parent);
					if let Some(parent) = old_parent.parent.as_ref().and_then(|x| x.upgrade()) {
						let items: Vec<_> = parent.children.clone();
						let selected_index = items
							.iter()
							.enumerate()
							.find(|(_, x)| Arc::ptr_eq(&old_parent, x))
							.map(|(i, _)| i)
							.unwrap_or(0);
						table.set_items(items);
						lock.current_parent = parent;
						table.set_selected_item(selected_index);
					}
				},
			);
		});
	}

	let mut version_table = TableView::<VersionItem, VersionColumn>::new()
		.column(VersionColumn::Name, "Name", |c| c)
		.column(VersionColumn::OriginalSize, "OSz", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(VersionColumn::GroupDsize, "Gdsz", |c| {
			c.width(8).align(HAlign::Right)
		})
		.column(VersionColumn::LocalDsize, "Ldsz", |c| {
			c.width(8).align(HAlign::Right)
		});

	siv.add_fullscreen_layer(
		LinearLayout::new(Orientation::Vertical)
			.child(
				Panel::new(table.with_name("contents"))
					.title("Repository contents")
					.full_height(),
			)
			.child(
				Panel::new(version_table.with_name("versions"))
					.title("File versions")
					.full_height(),
			)
			.full_screen(),
	);

	siv.add_layer(
		Dialog::new()
			.title("Loading")
			.content(
				ArchiveProgress::Opening
					.applied(ProgressBar::new())
					.with_name("progress")
					.min_width(16),
			)
			.button("Cancel", |siv| {
				siv.quit();
			}),
	);

	siv.run();
	Ok(())
}
