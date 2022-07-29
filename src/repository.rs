use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use rmp_serde;

use super::compress;
use super::crypto;
use super::crypto::Key;
use super::segments::{Id, Segment, SegmentReader};
use super::structs::{Archive, Manifest};

pub struct Repository {
	root: PathBuf,
}

impl Repository {
	pub fn open<P: Into<PathBuf>>(p: P) -> io::Result<Self> {
		Ok(Self { root: p.into() })
	}

	fn iter_segment_files(&self) -> io::Result<ReverseSegmentFileIter<'_>> {
		ReverseSegmentFileIter::new(&self.root)
	}

	pub fn get(&self, key: &Id) -> io::Result<Option<Vec<u8>>> {
		for item in self.iter_segment_files()? {
			let item = item?;
			let mut rdr = SegmentReader::new(fs::File::open(item)?);
			while let Some(segment) = rdr.read()? {
				match segment {
					Segment::Put {
						key: ref seg_key,
						ref data,
					} => {
						if seg_key == key {
							return Ok(Some(data.to_owned().to_vec()));
						}
					}
					Segment::Delete { key: ref seg_key } => {
						if seg_key == key {
							return Ok(None);
						}
					}
					Segment::Commit => (),
				}
			}
		}
		Ok(None)
	}

	fn read_raw<K: crypto::Key>(&self, id: &Id, key: &mut K) -> io::Result<Vec<u8>> {
		let data = match self.get(id)? {
			Some(v) => v,
			None => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					format!("object with id {:?} not found in repository", id),
				))
			}
		};
		let data = key.decrypt(&data[..]);
		let compressor = match compress::detect_compression(&data[..]) {
			None => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("unknown decompressor ({:?})", &data[..2]),
				))
			}
			Some(v) => v,
		};
		let data = compressor.decompress(&data[..])?;
		Ok(data)
	}

	fn read_object<T: DeserializeOwned>(&self, id: &Id) -> io::Result<T> {
		let data = self.read_raw(id, &mut crypto::Plaintext::new())?;
		let item: T = rmp_serde::from_read(&data[..])
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))?;
		Ok(item)
	}

	pub fn read_archive(&self, id: &Id) -> io::Result<Archive> {
		self.read_object(id)
	}

	pub fn read_manifest(&self) -> io::Result<Option<Manifest>> {
		let manifest_data = match self.get(&Id::zero())? {
			Some(v) => v,
			None => return Ok(None),
		};
		let key = crypto::Plaintext::new();
		let manifest_data = key.decrypt(&manifest_data[..]);
		let compressor = match compress::detect_compression(&manifest_data[..]) {
			None => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("unknown decompressor ({:?})", &manifest_data[..2]),
				))
			}
			Some(v) => v,
		};
		let manifest_data = compressor.decompress(&manifest_data[..])?;
		let manifest: Manifest = rmp_serde::from_read(&manifest_data[..])
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))?;
		Ok(Some(manifest))
	}
}

pub struct ReverseSegmentFileIter<'x> {
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
