use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use configparser::ini::Ini;

use bytes::Bytes;

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
			Self::InvalidConfigKey(section, name, err) => write!(f, ""),
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

		Ok(Self {
			root,
			config,
			segments_per_dir,
			latest_segment,
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
		let mut path = at.into();
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

	fn get_latest_segment(&self) -> io::Result<u64> {
		Ok(self.latest_segment)
	}
}

#[async_trait::async_trait(?Send)]
impl ObjectStore for FsStore {
	async fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes> {
		let id = id.as_ref();
		{
			let lock = self.segment_cache.read().unwrap();
			match lock.get(id) {
				Some(Some((segment_no, offset))) => {
					let (dir, file) = split_fileno(self.segments_per_dir, *segment_no);
					let mut path = self.root.clone();
					path.push("data");
					path.push(dir.to_string());
					path.push(file.to_string());
					let mut file = io::BufReader::new(fs::File::open(path)?);
					file.seek(io::SeekFrom::Start(*offset))?;
					let segment = match read_segment(file) {
						Ok(v) => v,
						Err(e) => panic!(
							"failed to re-read cached segment in {}/{} @ {}: {:?}",
							dir, segment_no, offset, e
						),
					};
					match segment {
						Some(Segment::Put { ref key, ref data }) => {
							assert_eq!(key, id);
							return Ok(data.to_owned().to_vec().into());
						}
						_ => panic!("cache pointed at invalid segment"),
					}
				}
				Some(None) => {
					return Err(io::Error::new(
						io::ErrorKind::NotFound,
						format!("key {:?} not in repository", id),
					));
				}
				None => (),
			}
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

	async fn contains<K: AsRef<Id>>(&self, id: K) -> io::Result<bool> {
		let id = id.as_ref();
		{
			let lock = self.segment_cache.read().unwrap();
			match lock.get(id) {
				Some(entry) => return Ok(entry.is_some()),
				// not in cache, need to search
				None => (),
			}
		}
		Ok(self.search_chunk(id)?.is_some())
	}

	async fn get_repository_config_key(&self, key: &str) -> Option<String> {
		self.config.get("repository", key)
	}

	async fn check_all_segments(
		&self,
		mut progress: Option<&mut dyn ProgressSink>,
	) -> io::Result<()> {
		let max_segment_number = self.latest_segment;
		let segments_per_dir = self.segments_per_dir;
		let mut lock = self.segment_cache.write().unwrap();
		// as we're going to read *all* segments, we'll now clear the map
		// and reconstruct it.
		lock.clear();
		let mut data_path = self.root.clone();
		data_path.push("data");
		// we're popping that off in the first iteration
		data_path.push("XXXXXXXX");

		for segment_no in 0..=max_segment_number {
			let (dir, file) = split_fileno(segments_per_dir, segment_no);
			if file % segments_per_dir == 0 {
				progress.report(Progress::Range {
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
			let mut reader = SegmentReader::new(io::BufReader::new(reader));
			while let Some((offset, segment)) = reader.read_pos()? {
				match segment {
					Segment::Put { ref key, ref data } => {
						lock.insert(*key, Some((segment_no, offset)));
					}
					Segment::Delete { ref key } => {
						lock.insert(*key, None);
					}
					Segment::Commit => (),
				}
			}
		}
		progress.report(Progress::Complete);
		Ok(())
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
