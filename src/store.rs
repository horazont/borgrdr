//! Backend for accessing a borg repository
use std::io;
use std::io::BufRead;

use bytes::{Buf, Bytes};

use super::segments::Id;

pub trait ObjectStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes>;
}
