//! Backend for accessing a borg repository
use std::io;
use std::io::BufRead;

use bytes::{Buf, Bytes};

use super::progress::ProgressSink;
use super::segments::Id;

pub trait ObjectStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes>;
	fn contains<K: AsRef<Id>>(&self, id: K) -> io::Result<bool>;
	fn read_config(&self) -> io::Result<Bytes>;
	fn get_latest_segment(&self) -> io::Result<u64>;
	fn check_all_segments(&self, progress: Option<&mut dyn ProgressSink>) -> io::Result<()>;
}
