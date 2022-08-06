use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::{Context, Poll};

use configparser::ini::Ini;

use futures::stream::Stream;

use bytes::Bytes;

use super::hashindex::{MmapSegmentIndex, ReadSegmentIndex};
use super::progress::{Progress, ProgressSink};
use super::segments::{read_segment, Id, Segment, SegmentReader};
use super::store::ObjectStore;

#[derive(Debug)]
pub enum Error {
	InvalidConfig(String),
	MissingConfigKey(Cow<'static, str>, Cow<'static, str>),
	InvalidConfigKey(
		Cow<'static, str>,
		Cow<'static, str>,
		Box<dyn std::error::Error + Send + Sync + 'static>,
	),
	InaccessibleConfig(io::Error),
	InaccessibleIndex(io::Error),
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::InvalidConfig(err) => write!(f, "failed to parse repository config: {}", err),
			Self::MissingConfigKey(section, name) => write!(
				f,
				"missing repository config key: [{:?}] {:?}",
				section, name
			),
			Self::InvalidConfigKey(section, name, err) => write!(
				f,
				"malformed config key {:?} in section {:?}: {}",
				name, section, err
			),
			Self::InaccessibleConfig(err) => write!(f, "failed to read repository config: {}", err),
			Self::InaccessibleIndex(err) => write!(f, "failed to access repository index: {}", err),
		}
	}
}

impl std::error::Error for Error {}

fn get_config_key<T>(
	cfg: &Ini,
	f: impl Fn(&Ini, &str, &str) -> Result<Option<T>, String>,
	section: &'static str,
	name: &'static str,
) -> Result<T, Error> {
	match f(cfg, section, name) {
		Ok(Some(v)) => Ok(v),
		Ok(None) => Err(Error::MissingConfigKey(section.into(), name.into())),
		Err(e) => Err(Error::InvalidConfigKey(
			section.into(),
			name.into(),
			e.into(),
		)),
	}
}

fn split_fileno(segments_per_dir: u64, fileno: u64) -> (u64, u64) {
	(fileno / segments_per_dir, fileno)
}

pub struct FsStore {
	root: PathBuf,
	config: Ini,
	segments_per_dir: u64,
	latest_segment: u64,
	on_disk_index: Option<MmapSegmentIndex>,
	segment_cache: RwLock<HashMap<Id, Option<(u64, u64)>>>,
}

impl FsStore {
	pub fn open<P: Into<PathBuf>>(p: P) -> Result<Self, Error> {
		let root = p.into();
		let mut config_path = root.clone();
		config_path.push("config");
		let mut config = Ini::new();
		config
			.read(fs::read_to_string(config_path).map_err(Error::InaccessibleConfig)?)
			.map_err(Error::InvalidConfig)?;

		let segments_per_dir =
			get_config_key(&config, Ini::getuint, "repository", "segments_per_dir")?;

		let latest_segment = Self::find_latest_segment(&root).map_err(Error::InaccessibleIndex)?;
		let mut index = root.clone();
		index.push(format!("index.{}", latest_segment));
		let on_disk_index =
			Some(MmapSegmentIndex::open(&index).expect("failed to open on-disk index"));

		Ok(Self {
			root,
			config,
			segments_per_dir,
			latest_segment,
			on_disk_index,
			segment_cache: RwLock::new(HashMap::new()),
		})
	}

	fn iter_segment_files(&self) -> io::Result<ReverseSegmentFileIter<'_>> {
		Ok(ReverseSegmentFileIter::new(
			&self.root,
			self.latest_segment,
			self.segments_per_dir,
		))
	}

	fn read_single_segment(&self, id: &Id, segmentno: u64, offset: u64) -> io::Result<Bytes> {
		let mut path = self.root.clone();
		path.reserve(4 + 10 + 10);
		path.push("data");
		let (dir, fileno) = split_fileno(self.segments_per_dir, segmentno);
		path.push(dir.to_string());
		path.push(fileno.to_string());
		let mut file = fs::File::open(&path)?;
		file.seek(io::SeekFrom::Start(offset as u64))?;
		let file = io::BufReader::new(file);
		match read_segment(file)? {
			Some(Segment::Put { ref key, ref data }) => {
				if key != id {
					Err(io::Error::new(
						io::ErrorKind::InvalidData,
						format!(
							"chunk index pointed at {}/{} @ {}, which is a mismatching PUT",
							dir, fileno, offset
						),
					))
				} else {
					Ok(data.to_owned().to_vec().into())
				}
			}
			_ => Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!(
					"chunk index pointed at {}/{} @ {}, which is not a PUT",
					dir, fileno, offset
				),
			)),
		}
	}

	fn try_on_disk_cache(&self, id: &Id) -> Option<io::Result<Option<(u64, u64)>>> {
		let on_disk_index = self.on_disk_index.as_ref()?;
		let entry = match on_disk_index.get(id) {
			Some(v) => v,
			None => return Some(Ok(None)),
		};
		Some(Ok(Some((entry.segment as u64, entry.offset as u64))))
	}

	fn try_in_memory_cache(&self, id: &Id) -> Option<io::Result<Option<(u64, u64)>>> {
		let (segment, offset) = {
			let lock = self.segment_cache.read().unwrap();
			match lock.get(id)? {
				Some((segment, offset)) => (*segment, *offset),
				None => return Some(Ok(None)),
			}
		};
		Some(Ok(Some((segment, offset))))
	}

	fn search_chunk(&self, id: &Id) -> io::Result<Option<Bytes>> {
		let mut lock = self.segment_cache.write().unwrap();
		for item in self.iter_segment_files()? {
			let (fileno, item) = item?;
			let mut rdr = SegmentReader::new(io::BufReader::new(item));
			while let Some((offset, segment)) = rdr.read_pos()? {
				match segment {
					Segment::Put { ref key, ref data } => {
						if !lock.contains_key(key) {
							lock.insert(*key, Some((fileno, offset)));
						}
						if key == id {
							return Ok(Some(data.to_owned().to_vec().into()));
						}
					}
					Segment::Delete { ref key } => {
						if !lock.contains_key(key) {
							lock.insert(*key, None);
						}
						if key == id {
							return Ok(None);
						}
					}
					Segment::Commit => (),
				}
			}
		}
		Ok(None)
	}

	fn find_latest_segment<P: Into<PathBuf>>(at: P) -> io::Result<u64> {
		let path = at.into();
		let mut index_segment = None;
		let mut hints_segment = None;
		let mut integrity_segment = None;
		for item in fs::read_dir(path)? {
			let item = item?;
			let file_name = match item.file_name().into_string() {
				Ok(v) => v,
				Err(_) => continue,
			};
			if file_name.starts_with("index.") {
				index_segment = Some(file_name[6..].parse::<u64>().unwrap());
			} else if file_name.starts_with("hints.") {
				hints_segment = Some(file_name[6..].parse::<u64>().unwrap());
			} else if file_name.starts_with("integrity.") {
				integrity_segment = Some(file_name[10..].parse::<u64>().unwrap());
			}
		}

		let index_segment = match index_segment {
			Some(v) => v,
			None => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					"failed to find index file",
				))
			}
		};
		let hints_segment = hints_segment.unwrap_or(index_segment);
		let integrity_segment = integrity_segment.unwrap_or(index_segment);
		if index_segment != hints_segment || hints_segment != integrity_segment {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!(
					"inconsistent segment numbers: {}/{}/{}",
					index_segment, hints_segment, integrity_segment
				),
			));
		}

		Ok(index_segment)
	}

	fn contains_inner(&self, id: &Id) -> io::Result<bool> {
		let id = id.as_ref();
		match self
			.try_on_disk_cache(id)
			.or_else(|| self.try_in_memory_cache(id))
		{
			Some(Ok(Some(_))) => return Ok(true),
			Some(Ok(None)) => return Ok(false),
			Some(Err(e)) => return Err(e),
			// caches are all inconclusive, do exhaustive search
			None => (),
		}
		Ok(self.search_chunk(id)?.is_some())
	}
}

#[async_trait::async_trait]
impl ObjectStore for Arc<FsStore> {
	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes> {
		let id = id.as_ref();
		match self
			.try_on_disk_cache(id)
			.or_else(|| self.try_in_memory_cache(id))
		{
			Some(Ok(Some((segment, offset)))) => {
				return self.read_single_segment(id, segment, offset)
			}
			Some(Ok(None)) => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					format!("key {:?} not in repository", id),
				));
			}
			Some(Err(e)) => return Err(e),
			// caches are all inconclusive, do exhaustive search
			None => (),
		}
		match self.search_chunk(id)? {
			Some(v) => return Ok(v),
			None => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					format!("key {:?} not in repository", id),
				))
			}
		}
	}

	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool> {
		self.contains_inner(id.as_ref())
	}

	async fn find_missing_chunks(&self, ids: Vec<Id>) -> io::Result<Vec<Id>> {
		let mut missing = Vec::new();
		for (i, id) in ids.into_iter().enumerate() {
			if !self.contains_inner(&id)? {
				missing.push(id);
			}
			// make sure we yield once in a while
			if i % 100 == 99 {
				tokio::task::yield_now().await;
			}
		}
		Ok(missing)
	}

	async fn get_repository_config_key(&self, key: &str) -> io::Result<Option<String>> {
		Ok(self.config.get("repository", key))
	}

	async fn check_all_segments(
		&self,
		mut progress: Option<Box<dyn ProgressSink + Send + 'static>>,
	) -> io::Result<()> {
		let max_segment_number = self.latest_segment;
		let segments_per_dir = self.segments_per_dir;
		let mut data_path = self.root.clone();
		data_path.push("data");
		// we're popping that off in the first iteration
		data_path.push("XXXXXXXX");

		let mut new_cache = HashMap::<_, Option<(u64, u64)>>::new();
		let mut update_cache = HashMap::<_, Option<(u64, u64)>>::new();
		for segment_no in 0..=max_segment_number {
			let (dir, file) = split_fileno(segments_per_dir, segment_no);
			if segment_no % 100 == 0 {
				progress.report(Progress::Range {
					// revresed!
					cur: segment_no,
					max: max_segment_number,
				});
				// this is the first segment of a new directory
				data_path.pop();
				data_path.push(dir.to_string());
			}

			data_path.push(file.to_string());
			let reader = match fs::File::open(&data_path) {
				Ok(v) => v,
				Err(e) if e.kind() == io::ErrorKind::NotFound => {
					// probably compacted away
					data_path.pop();
					continue;
				}
				Err(e) => return Err(e),
			};
			data_path.pop();
			tokio::task::yield_now().await;
			let mut reader = SegmentReader::new(io::BufReader::new(reader));
			while let Some((offset, segment)) = reader.read_pos()? {
				let (key, value) = match segment {
					Segment::Put { ref key, .. } => (*key, Some((segment_no, offset))),
					Segment::Delete { ref key } => (*key, None),
					Segment::Commit => {
						for (key, value) in update_cache.drain() {
							new_cache.insert(key, value);
						}
						update_cache.clear();
						continue;
					}
				};
				update_cache.insert(key, value);
				tokio::task::yield_now().await;
			}
		}

		{
			let mut lock = self.segment_cache.write().unwrap();
			*lock = new_cache;
		}
		tokio::task::yield_now().await;
		if let Some(on_disk_index) = self.on_disk_index.as_ref() {
			let lock = self.segment_cache.read().unwrap();
			for (key, value) in lock.iter() {
				let claim = on_disk_index.contains(key);
				if claim && value.is_none() {
					return Err(io::Error::new(io::ErrorKind::InvalidData, format!("on-disk segment cache error: claims {:?} exists, but I found an authoritative delete!", key)));
				}
				if !claim && value.is_some() {
					return Err(io::Error::new(io::ErrorKind::InvalidData, format!("on-disk segment cache error: claims {:?} does not exist, but I found an authoritative put!", key)));
				}
			}
		}
		tokio::task::yield_now().await;
		progress.report(Progress::Complete);
		Ok(())
	}

	type ChunkStream = ChunkStream;

	fn stream_chunks(&self, chunks: Vec<Id>) -> io::Result<Self::ChunkStream> {
		Ok(Self::ChunkStream {
			ids: chunks.into_iter(),
			store: Arc::clone(self),
			request: None,
		})
	}
}

pub struct ChunkStream {
	store: Arc<FsStore>,
	ids: std::vec::IntoIter<Id>,
	request: Option<Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>>>,
}

impl Stream for ChunkStream {
	type Item = io::Result<Bytes>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		if self.request.is_none() {
			let id = match self.ids.next() {
				None => return Poll::Ready(None),
				Some(id) => id,
			};
			let store = Arc::clone(&self.store);
			self.request = Some(Box::pin(async move { store.retrieve(id).await }));
		}

		match self.request.as_mut().unwrap().as_mut().poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(v) => {
				self.request = None;
				Poll::Ready(Some(v))
			}
		}
	}
}

struct ReverseSegmentFileIter<'x> {
	root: &'x Path,
	segment: Option<u64>,
	segments_per_dir: u64,
}

impl<'x> ReverseSegmentFileIter<'x> {
	fn new(root: &'x Path, segment: u64, segments_per_dir: u64) -> Self {
		Self {
			root,
			segment: Some(segment + 1),
			segments_per_dir,
		}
	}
}

impl<'x> Iterator for ReverseSegmentFileIter<'x> {
	type Item = io::Result<(u64, fs::File)>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut buf = self.root.to_path_buf();
		buf.push("data");
		buf.push("XXXXXXXX");
		buf.push("XXXX");

		while let Some(segment) = self.segment.as_mut().and_then(|x| {
			let next = x.checked_sub(1)?;
			*x = next;
			Some(next)
		}) {
			let (dir, file) = split_fileno(self.segments_per_dir, segment);
			buf.pop();
			buf.pop();
			buf.push(dir.to_string());
			buf.push(file.to_string());

			let fileobj = match fs::File::open(&buf) {
				Ok(v) => v,
				Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
				Err(e) => return Some(Err(e)),
			};
			return Some(Ok((segment, fileobj)));
		}
		None
	}
}
