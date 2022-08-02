//! Backend for accessing a borg repository
use std::io;
use std::io::BufRead;

use bytes::{Buf, Bytes};

use super::segments::Id;

pub trait ObjectStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes>;
	fn contains<K: AsRef<Id>>(&self, id: K) -> io::Result<bool>;
	fn read_config(&self) -> io::Result<Bytes>;
	fn check_all_segments(&self) -> io::Result<()>;
}
