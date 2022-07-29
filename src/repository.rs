use std::io;

use serde::de::DeserializeOwned;

use bytes::Bytes;

use rmp_serde;

use super::compress;
use super::crypto;
use super::crypto::Key;
use super::segments::Id;
use super::store::ObjectStore;
use super::structs::{Archive, Manifest};

pub trait SecretProvider {
	fn prompt_secret(&mut self) -> io::Result<Bytes>;
}

pub struct Repository<S> {
	store: S,
	manifest: Manifest,
	key: Box<dyn Key>,
}

impl<S: ObjectStore> Repository<S> {
	pub fn open(store: S) -> io::Result<Self> {
		// once we implement crypto support, we need two things:
		// - the ability to query the repokey from the ObjectStore
		// - a way to ask for a passphrase (by passing a callback)
		let key = Box::new(crypto::Plaintext::new());
		let manifest = Self::read_manifest(&store, &key)?;
		Ok(Self {
			store,
			manifest,
			key,
		})
	}

	fn read_raw(&self, id: &Id) -> io::Result<Vec<u8>> {
		let data = self.store.retrieve(id)?;
		let data = self.key.decrypt(&data[..]);
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
		let data = self.read_raw(id)?;
		rmp_serde::from_read(&data[..]).map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
	}

	pub fn read_archive(&self, id: &Id) -> io::Result<Archive> {
		self.read_object(id)
	}

	fn read_manifest(store: &S, key: &impl Key) -> io::Result<Manifest> {
		let data = store.retrieve(&Id::zero())?;
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
		rmp_serde::from_read(&data[..]).map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
	}

	pub fn manifest(&self) -> &Manifest {
		&self.manifest
	}
}
