// Idea:
// - build tree of all files in all archives
// - collapse identical subtrees and note their dedup-size
// - identify trees by hash(chunks), where chunks are joined in a unique manner

// Target view:

// initial (nothing expanded)
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
///  `/` (338.2 GiB, 1 version)
///  `/` (328.3 GiB, 12 versions)
///  `/` (328.2 GiB, 1 version)

// then one can either expand the path (moving the version distinguishing down one layer)
/// `/`
/// | `/data` (.., N versions)
/// | `/data` (.., M versions)
/// | `/etc` ..

// or a version
/// `/` (.. GiB, 1 version)
/// | `/data` ..
/// | ..
/// `/` (.. GiB" 12 versions)
/// `/` ..
// size displayed should be dedup-size if all the versions were conflated, i.e. the number of chunks unique outside of the set of the versions which have been conflated
// (amortized size, i.e. the above dedup-size divided by number of versions could also be displayed)

// Implementation
// 1 worker thread which scans the archives and pushes dir entries with chunk lists into more worker threads
// 1 worker thread which receives dir entries and builds the tree -- main workhors
use std::env::args;
use std::fmt;
use std::io;
use std::sync::Arc;

use ring::digest::{Context as DigestContext, SHA256};

use bytes::{BufMut, Bytes, BytesMut};

use anyhow::{Context, Result};

use tokio::sync::mpsc;

use futures::stream::StreamExt;

use chrono::{DateTime, TimeZone, Utc};

use borgrdr::repository::Repository;
use borgrdr::rpc::RpcStoreClient;
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

#[derive(Debug)]
struct Version {
	name: String,
	// TODO: convert to DateTime<Utc>
	timestamp: String,
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
	archive_sink: mpsc::Sender<(Arc<Version>, mpsc::Receiver<FileEntry>)>,
) -> Result<(), io::Error> {
	let manifest = repo.manifest();
	for v in manifest.archives().values() {
		let (version, items) = {
			let archive = repo.read_archive(v.id()).await?;
			let version = Arc::new(Version {
				name: archive.name().into(),
				timestamp: archive.start_time().into(),
			});
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
	}
	Ok(())
}

enum VersionedNode {
	Directory {
		children: HashMap<Bytes, Box<VersionedNode>>,
	},
	Regular {
		chunks: Vec<Chunk>,
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

impl From<FileEntry> for VersionedNode {
	fn from(other: FileEntry) -> Self {
		match other.payload {
			FileData::Directory {} => Self::new_directory(),
			FileData::Regular { chunks, .. } => Self::Regular { chunks },
			FileData::Symlink { target_path, .. } => Self::Symlink {
				target: target_path,
			},
		}
	}
}

impl VersionedNode {
	pub fn new_directory() -> Self {
		Self::Directory {
			children: HashMap::new(),
		}
	}

	pub fn insert_node_at_path<'x>(&'x mut self, mut item: FileEntry) -> &'x mut VersionedNode {
		match self {
			Self::Directory { children } => {
				let entry_name = split_first_segment(&mut item.path);
				let need_dir = item.path.len() > 0;
				// TODO: avoid allocation here if possible...
				let next = match children.entry(entry_name) {
					Entry::Vacant(v) => {
						if need_dir {
							v.insert(Box::new(VersionedNode::new_directory()))
						} else {
							return v.insert(Box::new(item.into()));
						}
					}
					Entry::Occupied(o) => o.into_mut(),
				};
				next.insert_node_at_path(item)
			}
			_ => panic!("node type conflict"),
		}
	}
}

struct VersionedTree {
	version: Version,
	root: VersionedNode,
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
		children: HashMap<Bytes, Box<HashedNode>>,
	},
	Regular {
		chunks: Vec<Chunk>,
	},
	Symlink {
		target: Bytes,
	},
}

impl HashedNodeData {
	fn osize(&self) -> u64 {
		match self {
			Self::Directory { children } => children.values().map(|x| x.osize).sum(),
			Self::Regular { chunks } => chunks.iter().map(|chunk| chunk.size()).sum(),
			Self::Symlink { .. } => 0,
		}
	}

	fn csize(&self) -> u64 {
		match self {
			Self::Directory { children } => children.values().map(|x| x.csize).sum(),
			Self::Regular { chunks } => chunks.iter().map(|chunk| chunk.csize()).sum(),
			Self::Symlink { .. } => 0,
		}
	}

	fn unique_chunks(&self) -> HashSet<Chunk> {
		match self {
			Self::Directory { children } => {
				let mut result = HashSet::new();
				for child in children.values() {
					result.extend(&child.unique_chunks);
				}
				result
			}
			Self::Regular { chunks } => chunks.iter().cloned().collect(),
			Self::Symlink { .. } => HashSet::new(),
		}
	}

	fn content_hash(&self) -> Bytes {
		let mut buf = BytesMut::new();
		match self {
			Self::Directory { children } => {
				buf.put_u8(0x00);
				for (name, child) in children.iter() {
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
					buf.put_slice(&chunk.id().0[..]);
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

	fn split_for_merge(self) -> (MergedNodeData, Option<HashMap<Bytes, Box<HashedNode>>>) {
		match self {
			Self::Directory { children } => (MergedNodeData::Directory {}, Some(children)),
			Self::Regular { chunks } => (MergedNodeData::Regular { chunks }, None),
			Self::Symlink { target } => (MergedNodeData::Symlink { target }, None),
		}
	}
}

impl From<VersionedNode> for HashedNodeData {
	fn from(other: VersionedNode) -> Self {
		match other {
			VersionedNode::Directory { children } => Self::Directory {
				children: children
					.into_iter()
					.map(|(path, node)| (path, Box::new((*node).into())))
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
	osize: u64,
	csize: u64,
	unique_chunks: HashSet<Chunk>,
	data: HashedNodeData,
}

impl From<VersionedNode> for HashedNode {
	fn from(other: VersionedNode) -> Self {
		let data: HashedNodeData = other.into();
		let unique_chunks = data.unique_chunks();
		Self {
			osize: data.osize(),
			csize: data.csize(),
			unique_chunks,
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
	Regular { chunks: Vec<Chunk> },
	Symlink { target: Bytes },
}

struct MergedNodeVersion {
	data: MergedNodeData,
	versions: Vec<Arc<Version>>,
	osize: u64,
	csize: u64,
	unique_chunks: HashSet<Chunk>,
}

struct MergedNode {
	children: HashMap<Bytes, MergedNode>,
	versions: HashMap<Bytes, MergedNodeVersion>,
}

static INDENT: &'static str = "│ ";
static INDENT_LAST: &'static str = "  ";

impl MergedNode {
	fn new() -> Self {
		Self {
			children: HashMap::new(),
			versions: HashMap::new(),
		}
	}

	fn merge(&mut self, version: Arc<Version>, node: HashedNode) {
		let children = match self.versions.entry(node.content_hash) {
			Entry::Occupied(mut o) => {
				o.get_mut().versions.push(Arc::clone(&version));
				assert_eq!(o.get().osize, node.osize);
				let (_, children) = node.data.split_for_merge();
				children
			}
			Entry::Vacant(v) => {
				let (data, children) = node.data.split_for_merge();
				v.insert(MergedNodeVersion {
					data,
					versions: vec![Arc::clone(&version)],
					osize: node.osize,
					csize: node.csize,
					unique_chunks: node.unique_chunks,
				});
				children
			}
		};
		if let Some(children) = children {
			for (path, new_child) in children {
				let mut own_child = match self.children.entry(path) {
					Entry::Occupied(o) => o.into_mut(),
					Entry::Vacant(v) => v.insert(Self::new()),
				};
				own_child.merge(Arc::clone(&version), *new_child);
			}
		}
	}

	fn display<'f>(&self, f: &'f mut fmt::Formatter, indent: &mut String) -> fmt::Result {
		let total_versions: usize = self.versions.values().map(|x| x.versions.len()).sum();
		write!(
			f,
			"({} versions ({} distinct))\n",
			total_versions,
			self.versions.len()
		)?;
		for version in self.versions.values() {
			match &version.data {
				MergedNodeData::Directory { .. } => continue,
				MergedNodeData::Regular { .. } => {
					write!(
						f,
						"{}* file of size {} in {} archives\n",
						indent,
						version.osize,
						version.versions.len()
					)?;
					for subversion in version.versions.iter() {
						write!(f, "{}  - {:?}\n", indent, subversion)?;
					}
				}
				MergedNodeData::Symlink { target } => {
					write!(
						f,
						"{}* symlink to {:?} in {} archives\n",
						indent,
						target,
						version.versions.len()
					)?;
				}
			}
		}
		for (i, (name, child)) in self.children.iter().enumerate() {
			let name = String::from_utf8_lossy(name);
			let (this_indent, this_node) = if i == self.children.len() - 1 {
				(INDENT_LAST, "└ ")
			} else {
				(INDENT, "├ ")
			};
			write!(f, "{}{}{:?} ", indent, this_node, name)?;
			indent.push_str(this_indent);
			child.display(f, indent)?;
			indent.truncate(indent.len() - this_indent.len());
		}
		Ok(())
	}
}

impl fmt::Display for MergedNode {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		let mut indent = String::new();
		self.display(f, &mut indent)
	}
}

async fn hasher(
	mut src: mpsc::Receiver<(Arc<Version>, mpsc::Receiver<FileEntry>)>,
	dst: mpsc::Sender<(Arc<Version>, HashedNode)>,
) {
	while let Some((version, mut item_source)) = src.recv().await {
		log::info!("processing archive {:?}", version);
		let mut root = VersionedNode::new_directory();
		while let Some(item) = item_source.recv().await {
			root.insert_node_at_path(item);
		}
		let root: HashedNode = root.into();
		dst.send((version, root)).await.unwrap();
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	env_logger::init();
	let argv: Vec<String> = args().collect();

	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&argv[1])
		.await
		.with_context(|| format!("failed to open repository"))?;

	let (entry_sink, archive_source) = mpsc::channel(128);
	let (hashed_sink, mut hashed_source) = mpsc::channel(2);
	let reader = tokio::spawn(read_entries(repo, entry_sink));
	let hasher = tokio::spawn(hasher(archive_source, hashed_sink));

	let mut merged = MergedNode::new();
	while let Some((version, hashed_tree)) = hashed_source.recv().await {
		merged.merge(version, hashed_tree);
	}
	println!("{}", merged);

	hasher.await.unwrap();
	reader.await.unwrap()?;
	let _: Result<_, _> = backend.wait_for_shutdown().await;

	Ok(())
}
