use std::fmt;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;

use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

use serde::de::DeserializeOwned;

use bytes::{Buf, Bytes};

use rmp_serde;

use tokio_util::codec::FramedRead;

use super::compress;
use super::crypto;
use super::crypto::Key;
use super::rmp_codec::MpCodec;
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

impl<S: ObjectStore + Send + Sync + 'static> Repository<S> {
	pub async fn open(
		store: S,
		secret_provider: Box<dyn PassphraseProvider>,
	) -> Result<Self, Error> {
		// once we implement crypto support, we need two things:
		// - the ability to query the repokey from the ObjectStore
		// - a way to ask for a passphrase (by passing a callback)
		let repokey_data = match store.get_repository_config_key("key").await {
			Ok(v) => v,
			Err(e) => return Err(Error::InaccessibleConfig(e)),
		};
		let crypto_ctx = crypto::Context::new();
		let manifest = match Self::read_manifest(
			&store,
			repokey_data.as_ref().map(|x| x.as_ref()),
			&crypto_ctx,
			&secret_provider,
		)
		.await
		{
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

	fn detect_crypto(
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

	async fn read_object<T: DeserializeOwned>(&self, id: &Id) -> io::Result<T> {
		let data = self.store.retrieve(id).await?;
		self.decode_object(&data[..])
	}

	fn decode_raw(&self, data: &[u8]) -> io::Result<Bytes> {
		let key = Self::detect_crypto(
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

	pub async fn read_archive(&self, id: &Id) -> io::Result<Archive> {
		self.read_object(id).await
	}

	async fn read_manifest(
		store: &S,
		repokey_data: Option<&str>,
		crypto_ctx: &crypto::Context,
		passphrase: &Box<dyn PassphraseProvider>,
	) -> io::Result<Manifest> {
		let data = store.retrieve(&Id::zero()).await?;
		let key = Self::detect_crypto(repokey_data, crypto_ctx, passphrase, &data[..])?;
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

	pub fn open_stream<'x>(
		&'x self,
		ids: Vec<Id>,
	) -> io::Result<StreamReader<'x, S, S::ChunkStream>> {
		Ok(StreamReader {
			repo: self,
			curr: None,
			src: self.store.stream_chunks(ids)?,
		})
	}

	pub fn archive_items<'x>(
		&'x self,
		ids: Vec<Id>,
	) -> io::Result<FramedRead<StreamReader<'x, S, S::ChunkStream>, MpCodec<ArchiveItem>>> {
		Ok(FramedRead::new(self.open_stream(ids)?, MpCodec::new()))
	}
}

pin_project_lite::pin_project! {
	#[project = StreamReaderProj]
	pub struct StreamReader<'x, S, I> {
		repo: &'x Repository<S>,
		curr: Option<Bytes>,
		#[pin]
		src: I,
	}
}

impl<'p, 'x, S: ObjectStore + Send + Sync + 'static, I: Stream<Item = io::Result<Bytes>>>
	StreamReaderProj<'p, 'x, S, I>
{
	fn poll_next_object(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Bytes>>> {
		//println!("StreamReader looking for next object");
		match self.src.as_mut().poll_next(cx) {
			Poll::Ready(Some(Ok(v))) => Poll::Ready(Ok(Some(self.repo.decode_raw(&v)?.into()))),
			Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
			Poll::Ready(None) => Poll::Ready(Ok(None)),
			Poll::Pending => Poll::Pending,
		}
	}
}

impl<'p, 'x, S: ObjectStore + Send + Sync + 'static, I: Stream<Item = io::Result<Bytes>>> AsyncRead
	for StreamReader<'x, S, I>
{
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		//println!("StreamReader read({})", buf.remaining());
		match self.as_mut().poll_fill_buf(cx) {
			Poll::Ready(Ok(my_buf)) => {
				//println!("StreamReader got {}", my_buf.len());
				let len = my_buf.len().min(buf.remaining());
				buf.put_slice(&my_buf[..len]);
				//println!("StreamReader advanced by {}, remaining {}", len, my_buf.len() - len);
				self.consume(len);
				Poll::Ready(Ok(()))
			}
			Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
			Poll::Pending => Poll::Pending,
		}
	}
}

impl<'p, 'x, S: ObjectStore + Send + Sync + 'static, I: Stream<Item = io::Result<Bytes>>>
	AsyncBufRead for StreamReader<'x, S, I>
{
	fn consume(self: Pin<&mut Self>, amt: usize) {
		if amt == 0 {
			return;
		}
		let this = self.project();
		// unwrap: if amt > 0, the caller must've called fill_buf and received
		// a non-empty buffer; otherwise, it's ok to panic.
		let curr = this.curr.as_mut().unwrap();
		curr.advance(amt);
		if curr.remaining() == 0 {
			*this.curr = None;
		}
	}

	fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
		let mut this = self.project();
		loop {
			match this.curr.as_ref().map(|x| x.remaining() == 0) {
				// empty, need to get more
				Some(true) | None => {
					// more efficient when we're called again after Pending'
					*this.curr = None;
					*this.curr = match this.poll_next_object(cx) {
						Poll::Pending => return Poll::Pending,
						// still loop on in case this buffer is empty...
						Poll::Ready(Ok(Some(buf))) => Some(buf),
						Poll::Ready(Ok(None)) => return Poll::Ready(Ok(&[])),
						Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
					}
				}
				Some(false) => break,
			}
		}
		Poll::Ready(Ok(this.curr.as_ref().unwrap().chunk()))
	}
}
