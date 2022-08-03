use std::fmt;
use std::io;
use std::io::BufRead;

use serde::de::DeserializeOwned;

use bytes::{Buf, Bytes};

use rmp_serde;

use configparser::ini::Ini;

use super::compress;
use super::crypto;
use super::crypto::Key;
use super::segments::Id;
use super::store::ObjectStore;
use super::structs::{Archive, ArchiveItem, Manifest};

#[derive(Debug)]
pub enum Error {
	InvalidConfig(String),
	NonUtf8Config,
	InaccessibleConfig(io::Error),
	ManifestInaccessible(io::Error),
	ManifestVersionNotSupported(u8),
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::InvalidConfig(err) => write!(f, "failed to parse repository config: {}", err),
			Self::NonUtf8Config => write!(f, "failed to parse repository config: malformed utf-8"),
			Self::InaccessibleConfig(err) => write!(f, "failed to read repository config: {}", err),
			Self::ManifestInaccessible(err) => write!(f, "failed to read manifest: {}", err),
			Self::ManifestVersionNotSupported(v) => {
				write!(f, "unsupported manifest version: {}", v)
			}
		}
	}
}

impl std::error::Error for Error {}

pub trait PassphraseProvider {
	fn prompt_secret(&self) -> io::Result<Bytes>;
}

pub struct EnvPassphrase(Result<Bytes, (io::ErrorKind, &'static str)>);

impl EnvPassphrase {
	pub fn new() -> Self {
		Self(match std::env::var("BORG_PASSPHRASE") {
			Ok(v) => Ok(v.into_bytes().into()),
			Err(std::env::VarError::NotPresent) => {
				Err((io::ErrorKind::NotFound, "BORG_PASSPHRASE is not set"))
			}
			Err(std::env::VarError::NotUnicode(_)) => Err((
				io::ErrorKind::InvalidData,
				"BORG_PASSPHRASE is not valid unicode (sorry)",
			)),
		})
	}
}

impl PassphraseProvider for EnvPassphrase {
	fn prompt_secret(&self) -> io::Result<Bytes> {
		match self.0.as_ref() {
			Ok(v) => Ok(v.clone()),
			Err((kind, msg)) => Err(io::Error::new(*kind, *msg)),
		}
	}
}

struct SecretProvider<'x> {
	passphrase: &'x Box<dyn PassphraseProvider>,
	repokey_data: Option<&'x str>,
}

impl<'x> crypto::SecretProvider for SecretProvider<'x> {
	fn encrypted_key(&self) -> io::Result<Bytes> {
		match self.repokey_data.as_ref() {
			Some(v) => Ok(v.to_string().into()),
			None => Err(io::Error::new(
				io::ErrorKind::NotFound,
				"no repository key available",
			)),
		}
	}

	fn passphrase(&self) -> io::Result<Bytes> {
		self.passphrase.prompt_secret()
	}
}

pub struct Repository<S> {
	store: S,
	manifest: Manifest,
	repokey_data: Option<String>,
	secret_provider: Box<dyn PassphraseProvider>,
	crypto_ctx: crypto::Context,
}

impl<S: ObjectStore> Repository<S> {
	pub fn open(store: S, secret_provider: Box<dyn PassphraseProvider>) -> Result<Self, Error> {
		// once we implement crypto support, we need two things:
		// - the ability to query the repokey from the ObjectStore
		// - a way to ask for a passphrase (by passing a callback)
		let repokey_data = store.get_repository_config_key("key");
		let crypto_ctx = crypto::Context::new();
		let manifest = match Self::read_manifest(
			&store,
			repokey_data.as_ref().map(|x| x.as_ref()),
			&crypto_ctx,
			&secret_provider,
		) {
			Ok(v) => v,
			Err(e) => return Err(Error::ManifestInaccessible(e)),
		};
		if manifest.version() != 1 {
			return Err(Error::ManifestVersionNotSupported(manifest.version()));
		}
		Ok(Self {
			store,
			manifest,
			secret_provider,
			crypto_ctx,
			repokey_data,
		})
	}

	pub fn store(&self) -> &S {
		&self.store
	}

	pub fn store_mut(&mut self) -> &mut S {
		&mut self.store
	}

	fn detect_crypto(
		store: &S,
		repokey_data: Option<&str>,
		crypto_ctx: &crypto::Context,
		passphrase: &Box<dyn PassphraseProvider>,
		for_data: &[u8],
	) -> io::Result<Box<dyn Key>> {
		crypto::detect_crypto(
			crypto_ctx,
			for_data,
			&SecretProvider {
				repokey_data,
				passphrase,
			},
		)
	}

	fn read_object<T: DeserializeOwned>(&self, id: &Id) -> io::Result<T> {
		let data = self.store.retrieve(id)?;
		self.decode_object(&data[..])
	}

	fn decode_raw(&self, data: &[u8]) -> io::Result<Bytes> {
		let key = Self::detect_crypto(
			&self.store,
			self.repokey_data.as_ref().map(|x| x.as_ref()),
			&self.crypto_ctx,
			&self.secret_provider,
			data,
		)?;
		let data = key.decrypt(&data[..])?;
		let compressor = match compress::detect_compression(&data[..]) {
			None => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("unknown decompressor ({:?})", &data[..2]),
				))
			}
			Some(v) => v,
		};
		compressor.decompress(&data[..]).map(|x| x.into())
	}

	fn decode_object<T: DeserializeOwned>(&self, data: &[u8]) -> io::Result<T> {
		let data = self.decode_raw(data)?;
		rmp_serde::from_read(&data[..]).map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
	}

	pub fn read_archive(&self, id: &Id) -> io::Result<Archive> {
		self.read_object(id)
	}

	fn read_manifest(
		store: &S,
		repokey_data: Option<&str>,
		crypto_ctx: &crypto::Context,
		passphrase: &Box<dyn PassphraseProvider>,
	) -> io::Result<Manifest> {
		let data = store.retrieve(&Id::zero())?;
		let key = Self::detect_crypto(store, repokey_data, crypto_ctx, passphrase, &data[..])?;
		let data = key.decrypt(&data[..])?;
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

	pub fn open_stream<A: AsRef<Id>, I: Iterator<Item = A>>(
		&self,
		iter: I,
	) -> StreamReader<'_, S, I> {
		StreamReader {
			repo: self,
			curr: None,
			iter,
		}
	}

	pub fn archive_items<A: AsRef<Id>, I: Iterator<Item = A>>(
		&self,
		iter: I,
	) -> ItemIter<'_, S, I> {
		ItemIter {
			repo: self,
			stream: self.open_stream(iter),
			poisoned: false,
		}
	}
}

pub struct StreamReader<'x, S, I> {
	repo: &'x Repository<S>,
	curr: Option<Bytes>,
	iter: I,
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> StreamReader<'x, S, I> {
	fn next_object(&mut self) -> io::Result<Option<Bytes>> {
		let next_blob = self
			.iter
			.next()
			.map(|x| self.repo.store.retrieve(x))
			.transpose()?;
		match next_blob {
			None => return Ok(None),
			Some(blob) => Ok(Some(self.repo.decode_raw(&blob)?.into())),
		}
	}
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> io::Read for StreamReader<'x, S, I> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let src = self.fill_buf()?;
		let to_copy = src.len().min(buf.len());
		buf[..to_copy].copy_from_slice(&src[..to_copy]);
		self.consume(to_copy);
		Ok(to_copy)
	}
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> io::BufRead
	for StreamReader<'x, S, I>
{
	fn consume(&mut self, amt: usize) {
		if amt == 0 {
			return;
		}
		// unwrap: if amt > 0, the caller must've called fill_buf and received
		// a non-empty buffer; otherwise, it's ok to panic.
		let curr = self.curr.as_mut().unwrap();
		curr.advance(amt);
		if curr.remaining() == 0 {
			self.curr = None;
		}
	}

	fn fill_buf(&mut self) -> io::Result<&[u8]> {
		loop {
			// skip empty buffers
			match self.curr.as_ref().map(|x| x.remaining() == 0) {
				// empty
				Some(true) | None => match self.next_object()? {
					Some(next) => self.curr = Some(next),
					None => {
						self.curr = None;
						return Ok(&[]);
					}
				},
				Some(false) => break,
			}
		}
		Ok(self.curr.as_ref().unwrap().chunk())
	}
}

pub struct ItemIter<'x, S, I> {
	repo: &'x Repository<S>,
	stream: StreamReader<'x, S, I>,
	poisoned: bool,
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> Iterator for ItemIter<'x, S, I> {
	type Item = io::Result<ArchiveItem>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.poisoned {
			return None;
		}
		match self.stream.fill_buf() {
			Ok(v) if v.len() == 0 => return None,
			Err(e) => {
				self.poisoned = true;
				return Some(Err(e));
			}
			Ok(_) => (),
		}
		match rmp_serde::from_read(&mut self.stream)
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
		{
			Ok(v) => Some(Ok(v)),
			Err(e) => {
				self.poisoned = true;
				return Some(Err(e));
			}
		}
	}
}
