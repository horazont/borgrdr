use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::future::Future;
use std::io;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;

use log::{debug, trace};

use configparser::ini::Ini;

use bytes::Bytes;

use super::diag;
use super::diag::{DiagnosticsSink, Progress};
use super::hashindex::{MmapSegmentIndex, ReadSegmentIndex};
use super::segments;
use super::segments::{read_segment, Id, LogEntry, SegmentReader};
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

impl From<Error> for io::Error {
	fn from(other: Error) -> Self {
		match other {
			Error::InaccessibleConfig(e) => {
				io::Error::new(e.kind(), format!("inaccessible index: {}", e))
			}
			Error::InaccessibleIndex(e) => {
				io::Error::new(e.kind(), format!("inaccessible index: {}", e))
			}
			other => io::Error::new(io::ErrorKind::InvalidData, other),
		}
	}
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
		let on_disk_index = match std::env::var("BORGRDR_SKIP_INDEX")
			.as_ref()
			.map(|x| x.as_str())
		{
			Ok("yes") | Ok("true") | Ok("1") => None,
			_ => Some(MmapSegmentIndex::open(&index).expect("failed to open on-disk index")),
		};

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

	fn read_single_object(&self, id: &Id, segmentno: u64, offset: u64) -> io::Result<Bytes> {
		let mut path = self.root.clone();
		path.reserve(4 + 10 + 10);
		path.push("data");
		let (dir, fileno) = split_fileno(self.segments_per_dir, segmentno);
		path.push(dir.to_string());
		path.push(fileno.to_string());
		trace!(
			"reading object {:?} from segment {} in {:?} at offset {}",
			id,
			segmentno,
			path,
			offset
		);
		let mut file = fs::File::open(&path)?;
		file.seek(io::SeekFrom::Start(offset as u64))?;
		let file = io::BufReader::new(file);
		match read_segment(file)? {
			Some(LogEntry::Put { ref key, ref data }) => {
				if key != id {
					Err(io::Error::new(
						io::ErrorKind::InvalidData,
						format!(
							"segment index pointed at {}/{} @ {}, which is a mismatching PUT",
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
					"segment index pointed at {}/{} @ {}, which is not a PUT",
					dir, fileno, offset
				),
			)),
		}
	}

	fn try_on_disk_cache(&self, id: &Id) -> Option<io::Result<Option<(u64, u64)>>> {
		trace!("searching for {:?} in on-disk cache", id);
		let on_disk_index = self.on_disk_index.as_ref()?;
		let entry = match on_disk_index.get(id) {
			Some(v) => v,
			None => return Some(Ok(None)),
		};
		let segment = entry.segment as u64;
		let offset = entry.offset as u64;
		trace!(
			"found {:?} via in-memory cache in segment {} at {}",
			id,
			segment,
			offset
		);
		Some(Ok(Some((segment, offset))))
	}

	fn try_in_memory_cache(&self, id: &Id) -> Option<io::Result<Option<(u64, u64)>>> {
		trace!("searching for {:?} in in-memory cache", id);
		let (segment, offset) = {
			let lock = self.segment_cache.read().unwrap();
			match lock.get(id)? {
				Some((segment, offset)) => (*segment, *offset),
				None => return Some(Ok(None)),
			}
		};
		trace!(
			"found {:?} via in-memory cache in segment {} at {}",
			id,
			segment,
			offset
		);
		Some(Ok(Some((segment, offset))))
	}

	fn search_object(&self, id: &Id) -> io::Result<Option<Bytes>> {
		trace!("starting exhaustive search for {:?}", id);
		let mut lock = self.segment_cache.write().unwrap();
		// TODO: we should ignore everything before the first commit we
		// encounter... and we should also read the segment files themselves
		// in reverse order, I guess.
		for item in self.iter_segment_files()? {
			let (fileno, item) = item?;
			let mut rdr = SegmentReader::new(io::BufReader::new(item));
			loop {
				let (offset, log_entry) = rdr.read_pos();
				let log_entry = match log_entry? {
					Some(v) => v,
					None => break,
				};
				match log_entry {
					LogEntry::Put { ref key, ref data } => {
						if !lock.contains_key(key) {
							lock.insert(*key, Some((fileno, offset)));
						}
						if key == id {
							return Ok(Some(data.to_owned().to_vec().into()));
						}
					}
					LogEntry::Delete { ref key } => {
						if !lock.contains_key(key) {
							lock.insert(*key, None);
						}
						if key == id {
							return Ok(None);
						}
					}
					LogEntry::Commit => (),
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
		Ok(self.search_object(id)?.is_some())
	}

	async fn retrieve_inner(&self, id: Id) -> io::Result<Bytes> {
		match self
			.try_on_disk_cache(&id)
			.or_else(|| self.try_in_memory_cache(&id))
		{
			Some(Ok(Some((segment, offset)))) => {
				return self.read_single_object(&id, segment, offset)
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
		match self.search_object(&id)? {
			Some(v) => return Ok(v),
			None => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					format!("key {:?} not in repository", id),
				))
			}
		}
	}
}

#[async_trait::async_trait]
impl ObjectStore for Arc<FsStore> {
	type RetrieveFut<'x> = Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'x>>
		where Self: 'x;

	fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> Self::RetrieveFut<'_> {
		let id = *id.as_ref();
		Box::pin(self.retrieve_inner(id))
	}

	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool> {
		self.contains_inner(id.as_ref())
	}

	async fn find_missing_objects(&self, ids: Vec<Id>) -> io::Result<Vec<Id>> {
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
		mut progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()> {
		debug!("repository check started");
		let max_segment_number = self.latest_segment;
		let segments_per_dir = self.segments_per_dir;
		let mut data_path = self.root.clone();
		let mut ok = true;
		data_path.push("data");
		// we're popping that off in the first iteration
		data_path.push("XXXXXXXX");

		let mut new_cache = HashMap::<_, Option<(u64, u64)>>::new();
		let mut update_cache = HashMap::<_, Option<(u64, u64)>>::new();
		for segment_no in 0..=max_segment_number {
			trace!("checking segment {}", segment_no);
			let (dir, file) = split_fileno(segments_per_dir, segment_no);
			if segment_no % 100 == 0 {
				progress.progress(Progress::Range {
					// revresed!
					cur: segment_no,
					max: max_segment_number,
				});
			}
			data_path.pop();
			data_path.push(dir.to_string());

			data_path.push(file.to_string());
			let reader = match fs::File::open(&data_path) {
				Ok(v) => v,
				Err(e) if e.kind() == io::ErrorKind::NotFound => {
					// probably compacted away
					data_path.pop();
					continue;
				}
				Err(e) => {
					progress.log(
						diag::Level::Error,
						"segments",
						&format!(
							"failed to open segment file {} at {:?}: {}",
							segment_no, data_path, e
						),
					);
					ok = false;
					data_path.pop();
					continue;
				}
			};
			data_path.pop();
			tokio::task::yield_now().await;
			let mut reader = SegmentReader::new(io::BufReader::new(reader));
			loop {
				let (offset, item) = reader.read_pos();
				let log_entry = match item {
					Ok(Some(log_entry)) => log_entry,
					Ok(None) => break,
					Err(e) => {
						ok = false;
						match segments::Error::try_from(e) {
							Ok(segments::Error::CrcMismatch {
								unchecked,
								found,
								calculated,
							}) => {
								progress.log(diag::Level::Error, "segments", &format!("log entry crc mismatch in segment file {} at offset {}: 0x{:08x} (found) != 0x{:08x} (expected), but reading the log entry anyway", segment_no, offset, found, calculated));
								unchecked
							}
							Ok(other) => {
								progress.log(
									diag::Level::Error,
									"segments",
									&format!(
										"malformed log entry in segment file {} at offset {}: {}",
										segment_no, offset, other
									),
								);
								continue;
							}
							Err(other) => {
								progress.log(
									diag::Level::Error,
									"segments",
									&format!(
										"failed to read log entry in segment file {} at offset {}: {}",
										segment_no, offset, other
									),
								);
								continue;
							}
						}
					}
				};
				let (key, value) = match log_entry {
					LogEntry::Put { ref key, .. } => (*key, Some((segment_no, offset))),
					LogEntry::Delete { ref key } => (*key, None),
					LogEntry::Commit => {
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
			let mut expected_objects: HashSet<_> = on_disk_index.iter_keys().map(|x| *x).collect();
			{
				let lock = self.segment_cache.read().unwrap();
				for (key, value) in lock.iter() {
					let is_in_cache = expected_objects.remove(key);
					if is_in_cache && value.is_none() {
						progress.log(diag::Level::Error, "segment_index", &format!("on-disk segment index claims object {:?} does exist, but we found a DELETE", key));
						ok = false;
					}
					if !is_in_cache && value.is_some() {
						progress.log(diag::Level::Error, "segment_index", &format!("on-disk segment index claims object {:?} does not exist, but we found a PUT", key));
						ok = false;
					}
				}
			}
			for id in expected_objects.iter() {
				progress.log(diag::Level::Error, "segment_index", &format!("on-disk segment index claims object {:?} does exist, but we found no trace of it", id));
				ok = false;
			}
		}
		tokio::task::yield_now().await;
		progress.progress(Progress::Complete);
		match ok {
			true => Ok(()),
			false => Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"segment file check found errors",
			)),
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
