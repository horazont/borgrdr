use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::segments::{Id, Segment, SegmentReader};
use super::store::ObjectStore;

pub struct FsStore {
	root: PathBuf,
}

impl FsStore {
	pub fn open<P: Into<PathBuf>>(p: P) -> io::Result<Self> {
		Ok(Self { root: p.into() })
	}

	fn iter_segment_files(&self) -> io::Result<ReverseSegmentFileIter<'_>> {
		ReverseSegmentFileIter::new(&self.root)
	}
}

impl ObjectStore for FsStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes> {
		let id = id.as_ref();
		for item in self.iter_segment_files()? {
			let item = item?;
			let mut rdr = SegmentReader::new(fs::File::open(item)?);
			while let Some(segment) = rdr.read()? {
				match segment {
					Segment::Put { ref key, ref data } => {
						if key == id {
							return Ok(data.to_owned().to_vec().into());
						}
					}
					Segment::Delete { ref key } => {
						if key == id {
							return Err(io::Error::new(
								io::ErrorKind::NotFound,
								format!("key {:?} has been removed from the repository", id),
							));
						}
					}
					Segment::Commit => (),
				}
			}
		}
		Err(io::Error::new(
			io::ErrorKind::NotFound,
			format!("key {:?} does not exist", id),
		))
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
	type Item = io::Result<PathBuf>;

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
						return Some(Ok(buf));
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
								return Some(Ok(buf));
							}
							Ok(Some(v)) => v,
						};
						self.curr = Some((new_dir, new_file));
					}
					Ok(_) => {
						// nothing more to see, exit
					}
				};
				return Some(Ok(buf));
			}
			None => None,
		}
	}
}
