use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use bytes::Bytes;

use super::segments::{read_segment, Id, Segment, SegmentReader};
use super::store::ObjectStore;

pub struct FsStore {
	root: PathBuf,
	segment_cache: RwLock<HashMap<Id, Option<(u64, u64, u64)>>>,
}

impl FsStore {
	pub fn open<P: Into<PathBuf>>(p: P) -> io::Result<Self> {
		Ok(Self {
			root: p.into(),
			segment_cache: RwLock::new(HashMap::new()),
		})
	}

	fn iter_segment_files(&self) -> io::Result<ReverseSegmentFileIter<'_>> {
		ReverseSegmentFileIter::new(&self.root)
	}

	fn search_chunk(&self, id: &Id) -> io::Result<Option<Bytes>> {
		let mut lock = self.segment_cache.write().unwrap();
		for item in self.iter_segment_files()? {
			let (dir, file, item) = item?;
			let mut rdr = SegmentReader::new(fs::File::open(item)?);
			while let Some((offset, segment)) = rdr.read_pos()? {
				match segment {
					Segment::Put { ref key, ref data } => {
						if !lock.contains_key(key) {
							lock.insert(*key, Some((dir, file, offset)));
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
}

impl ObjectStore for FsStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes> {
		let id = id.as_ref();
		{
			let lock = self.segment_cache.read().unwrap();
			match lock.get(id) {
				Some(Some((dir, fileno, offset))) => {
					let mut path = self.root.clone();
					path.push("data");
					path.push(dir.to_string());
					path.push(fileno.to_string());
					let mut file = fs::File::open(path)?;
					file.seek(io::SeekFrom::Start(*offset))?;
					let segment = match read_segment(file) {
						Ok(v) => v,
						Err(e) => panic!(
							"failed to re-read cached segment in {}/{} @ {}: {:?}",
							dir, fileno, offset, e
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

	fn contains<K: AsRef<Id>>(&self, id: K) -> io::Result<bool> {
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

	fn read_config(&self) -> io::Result<Bytes> {
		let mut path = self.root.clone();
		path.push("config");
		let mut f = fs::File::open(path)?;
		let mut buf = Vec::new();
		f.read_to_end(&mut buf)?;
		Ok(buf.into())
	}

	fn check_all_segments(&self) -> io::Result<()> {
		let mut lock = self.segment_cache.write().unwrap();
		for item in self.iter_segment_files()? {
			let (dir, file, item) = item?;
			let mut rdr = SegmentReader::new(fs::File::open(item)?);
			while let Some((offset, segment)) = rdr.read_pos()? {
				match segment {
					Segment::Put { ref key, ref data } => {
						if !lock.contains_key(key) {
							lock.insert(*key, Some((dir, file, offset)));
						}
					}
					Segment::Delete { ref key } => {
						if !lock.contains_key(key) {
							lock.insert(*key, None);
						}
					}
					Segment::Commit => (),
				}
			}
		}
		Ok(())
	}
}

struct ReverseSegmentFileIter<'x> {
	root: &'x Path,
	curr: Option<(u64, u64)>,
}

impl<'x> ReverseSegmentFileIter<'x> {
	fn new(root: &'x Path) -> io::Result<Self> {
		let mut path = root.to_path_buf();
		path.push("data");
		let segment_dir = Self::find_largest(&path, None)?.unwrap();
		path.push(segment_dir.to_string());
		let file = Self::find_largest(&path, None)?.unwrap();
		Ok(Self {
			root,
			curr: Some((segment_dir, file)),
		})
	}

	fn find_largest(path: &Path, less_than: Option<u64>) -> io::Result<Option<u64>> {
		let mut largest: Option<u64> = None;
		for item in fs::read_dir(path)? {
			let item = item?;
			let num = match item
				.file_name()
				.to_str()
				.and_then(|x| x.parse::<u64>().ok())
			{
				Some(v) => v,
				None => continue,
			};
			if let Some(cutoff) = less_than.as_ref() {
				if num >= *cutoff {
					continue;
				}
			}

			if let Some(curr) = largest {
				if curr < num {
					largest = Some(num)
				}
			} else {
				largest = Some(num)
			}
		}
		Ok(largest)
	}
}

impl<'x> Iterator for ReverseSegmentFileIter<'x> {
	type Item = io::Result<(u64, u64, PathBuf)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.curr.take() {
			Some((dir, file)) => {
				let mut buf = self.root.to_path_buf();
				buf.push("data");
				buf.push(dir.to_string());
				let mut search_buf = buf.clone();
				buf.push(file.to_string());

				match Self::find_largest(&search_buf, Some(file)) {
					Err(e) => return Some(Err(e)),
					Ok(Some(v)) if v < file => {
						self.curr = Some((dir, v));
						return Some(Ok((dir, file, buf)));
					}
					Ok(_) => (),
				};

				search_buf.pop();
				match Self::find_largest(&search_buf, Some(dir)) {
					Err(e) => return Some(Err(e)),
					Ok(Some(new_dir)) if new_dir < dir => {
						search_buf.push(new_dir.to_string());
						let new_file = match Self::find_largest(&search_buf, None) {
							Err(e) => return Some(Err(e)),
							Ok(None) => {
								// nothing more to see, exit,
								return Some(Ok((dir, file, buf)));
							}
							Ok(Some(v)) => v,
						};
						self.curr = Some((new_dir, new_file));
					}
					Ok(_) => {
						// nothing more to see, exit
					}
				};
				return Some(Ok((dir, file, buf)));
			}
			None => None,
		}
	}
}
