use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use log::{debug, trace};

use futures::stream::{Stream, StreamExt};

use tokio::io::{AsyncBufRead, AsyncRead, ReadBuf};

use serde::de::DeserializeOwned;

use bytes::{Buf, Bytes};

use rmp_serde;

use tokio_util::codec::FramedRead;

use super::compress;
use super::crypto;
use super::crypto::Key;
use super::diag;
use super::diag::{DiagnosticsSink, Progress};
use super::rmp_codec::MpCodec;
use super::segments::Id;
use super::store::{ObjectStore, PrefetchStreamItem};
use super::structs::{Archive, ArchiveItem, Manifest};

#[derive(Debug)]
pub enum Error {
	InvalidConfig(String),
	NonUtf8Config,
	InaccessibleConfig(io::Error),
	ManifestInaccessible(io::Error),
	ManifestVersionNotSupported(u8),
}

impl From<Error> for io::Error {
	fn from(other: Error) -> Self {
		match other {
			Error::InaccessibleConfig(e) => {
				io::Error::new(e.kind(), format!("inaccessible index: {}", e))
			}
			Error::ManifestInaccessible(e) => {
				io::Error::new(e.kind(), format!("manifest inaccessible: {}", e))
			}
			other => io::Error::new(io::ErrorKind::InvalidData, other),
		}
	}
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
	passphrase: &'x Box<dyn PassphraseProvider + Send + Sync + 'static>,
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
	secret_provider: Box<dyn PassphraseProvider + Send + Sync + 'static>,
	crypto_ctx: crypto::Context,
}

impl<S: ObjectStore + Send + Sync + 'static> Repository<S> {
	pub fn into_store(self) -> S {
		self.store
	}

	pub async fn open(
		store: S,
		secret_provider: Box<dyn PassphraseProvider + Send + Sync + 'static>,
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
		passphrase: &Box<dyn PassphraseProvider + Send + Sync + 'static>,
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
		passphrase: &Box<dyn PassphraseProvider + Send + Sync + 'static>,
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
	) -> io::Result<StreamReader<'x, S, S::ObjectStream>> {
		Ok(StreamReader {
			repo: self,
			curr: None,
			src: self.store.stream_objects(ids)?,
		})
	}

	pub fn archive_items<'x>(
		&'x self,
		ids: Vec<Id>,
	) -> io::Result<FramedRead<StreamReader<'x, S, S::ObjectStream>, MpCodec<ArchiveItem>>> {
		Ok(FramedRead::new(self.open_stream(ids)?, MpCodec::new()))
	}

	pub async fn grouped_archive_items<
		'x,
		K: 'static + Copy + PartialEq + Eq + Send + Sync + Unpin,
		II: 'static + Iterator<Item = Id> + Send + Sync,
		I: 'static + Iterator<Item = (K, II)> + Send + Sync,
	>(
		&'x self,
		i: I,
	) -> io::Result<PrefetchedArchiveItems<'x, K, S::PrefetchStream<K, II, I>, S>> {
		let first = PrefetchStreamOuter {
			inner: Some(Box::pin(self.store.stream_objects_with_prefetch(i)?)),
		};
		let stream = first.await?;
		let inner = match stream {
			None => None,
			Some(stream) => Some(FramedRead::new(
				StreamReader {
					repo: self,
					src: stream,
					curr: None,
				},
				MpCodec::new(),
			)),
		};
		Ok(PrefetchedArchiveItems { inner })
	}

	pub async fn check_archives(
		&self,
		mut progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()> {
		let mut ok = true;
		for (i, (name, archive_hdr)) in self.manifest.archives().iter().enumerate() {
			progress.progress(Progress::Range {
				cur: i as u64,
				max: self.manifest.archives().len() as u64,
			});

			let archive_meta = match self.read_archive(archive_hdr.id()).await {
				Ok(v) => {
					debug!("check: opened archive {} (id={:?})", name, archive_hdr.id());
					v
				}
				Err(e) => {
					progress.log(
						diag::Level::Error,
						"archives",
						&format!(
							"failed to open archive {} (id={:?}): {}",
							name,
							archive_hdr.id(),
							e
						),
					);
					ok = false;
					continue;
				}
			};

			// accumulator for find_missing_objects calls -- batching them is much more efficient in RPC cases
			let mut idbuf = Vec::new();

			let mut archive_item_stream = match self.archive_items(archive_meta.items) {
				Ok(v) => v,
				Err(e) => {
					progress.log(
						diag::Level::Error,
						"archives",
						&format!(
							"failed to open archive metadata stream of archive {} (id={:?}): {}",
							name,
							archive_hdr.id(),
							e
						),
					);
					ok = false;
					continue;
				}
			};

			while let Some(item) = archive_item_stream.next().await {
				let item = match item {
					Ok(v) => v,
					Err(e) => {
						progress.log(
							diag::Level::Error,
							"archive_items",
							&format!("failed to read item metadata from {}: {}", name, e),
						);
						ok = false;
						continue;
					}
				};
				trace!("check: archive {}: found item {:?}", name, item.path());
				idbuf.extend(item.chunks().iter().map(|x| x.id()));
				if idbuf.len() >= 128 {
					let ids = idbuf.split_off(0);
					assert!(ids.len() > 0);
					for missing_id in self.store.find_missing_objects(ids).await? {
						ok = false;
						progress.log(
							diag::Level::Error,
							"archive_items",
							&format!("chunk {:?} is missing", missing_id),
						);
					}
				}
			}

			if idbuf.len() > 0 {
				for missing_id in self.store.find_missing_objects(idbuf).await? {
					ok = false;
					progress.log(
						diag::Level::Error,
						"archive_items",
						&format!("chunk {:?} is missing", missing_id),
					);
				}
			}
		}
		progress.progress(Progress::Complete);
		if ok {
			Ok(())
		} else {
			Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"one or more archive checks failed",
			))
		}
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

impl<'p, 'x, S, I> StreamReader<'x, S, I> {
	fn into_inner(self) -> (&'x Repository<S>, I) {
		(self.repo, self.src)
	}

	fn get_stream(&self) -> &I {
		&self.src
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

enum PrefetchStreamGroupState<M> {
	Running,
	Complete(Option<M>),
}

pub struct PrefetchStreamGroup<M, S> {
	inner: Pin<Box<S>>,
	identity: M,
	state: PrefetchStreamGroupState<M>,
}

impl<M: Unpin, T: Unpin, S: Stream<Item = PrefetchStreamItem<M, T>>> PrefetchStreamGroup<M, S> {
	fn into_next(self) -> Option<Self> {
		match self.state {
			PrefetchStreamGroupState::Running => panic!("stream group is not depleted yet"),
			PrefetchStreamGroupState::Complete(substate) => match substate {
				None => None,
				Some(next_metadata) => Some(Self {
					state: PrefetchStreamGroupState::Running,
					identity: next_metadata,
					inner: self.inner,
				}),
			},
		}
	}

	fn identity(&self) -> &M {
		&self.identity
	}
}

impl<M: Unpin, T: Unpin, S: Stream<Item = PrefetchStreamItem<M, T>>> Stream
	for PrefetchStreamGroup<M, S>
{
	type Item = io::Result<T>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		match self.state {
			PrefetchStreamGroupState::Running => (),
			PrefetchStreamGroupState::Complete(_) => return Poll::Ready(None),
		};

		match self.as_mut().inner.as_mut().poll_next(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(None) => {
				self.state = PrefetchStreamGroupState::Complete(None);
				Poll::Ready(None)
			}
			Poll::Ready(Some(PrefetchStreamItem::Metadata(metadata))) => {
				self.state = PrefetchStreamGroupState::Complete(Some(metadata));
				Poll::Ready(None)
			}
			Poll::Ready(Some(PrefetchStreamItem::Data(d))) => Poll::Ready(Some(d)),
		}
	}
}

pub struct PrefetchStreamOuter<S> {
	inner: Option<Pin<Box<S>>>,
}

impl<M, T, S: Stream<Item = PrefetchStreamItem<M, T>>> Future for PrefetchStreamOuter<S> {
	type Output = io::Result<Option<PrefetchStreamGroup<M, S>>>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.as_mut().inner.as_mut() {
			Some(inner) => match inner.as_mut().poll_next(cx) {
				Poll::Pending => Poll::Pending,
				Poll::Ready(None) => Poll::Ready(Ok(None)),
				Poll::Ready(Some(PrefetchStreamItem::Metadata(m))) => {
					Poll::Ready(Ok(Some(PrefetchStreamGroup {
						inner: self.inner.take().unwrap(),
						identity: m,
						state: PrefetchStreamGroupState::Running,
					})))
				}
				Poll::Ready(Some(PrefetchStreamItem::Data(Err(e)))) => Poll::Ready(Err(e)),
				Poll::Ready(Some(_)) => panic!("prefetch stream is not at beginning"),
			},
			None => Poll::Ready(Ok(None)),
		}
	}
}

pin_project_lite::pin_project! {
	#[project = PrefetchedArchiveItemsProj]
	pub struct PrefetchedArchiveItems<'x, K, Src, Store> {
		#[pin]
		inner: Option<FramedRead<StreamReader<'x, Store, PrefetchStreamGroup<K, Src>>, MpCodec<ArchiveItem>>>,
	}
}

impl<
		'x,
		K: PartialEq + Eq + Send + Sync + Unpin,
		Src: Stream<Item = PrefetchStreamItem<K, Bytes>>,
		Store: ObjectStore + Send + Sync + 'static,
	> PrefetchedArchiveItems<'x, K, Src, Store>
{
	pub fn next_group(self: Pin<&mut Self>) {
		let mut this = self.project();
		let (repo, inner) = match this.inner.take() {
			None => return,
			Some(v) => v.into_inner().into_inner(),
		};
		let new_inner = match inner.into_next() {
			None => return,
			Some(v) => v,
		};
		*this.inner = Some(FramedRead::new(
			StreamReader {
				repo,
				curr: None,
				src: new_inner,
			},
			MpCodec::new(),
		));
	}

	pub fn identity(&self) -> Option<&K> {
		Some(self.inner.as_ref()?.get_ref().get_stream().identity())
	}
}

impl<
		'x,
		K: PartialEq + Eq + Send + Sync + Unpin,
		Src: Stream<Item = PrefetchStreamItem<K, Bytes>>,
		Store: ObjectStore + Send + Sync + 'static,
	> Stream for PrefetchedArchiveItems<'x, K, Src, Store>
{
	type Item = io::Result<ArchiveItem>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.project();
		match this.inner.as_pin_mut() {
			None => Poll::Ready(None),
			Some(inner) => match inner.poll_next(cx) {
				Poll::Ready(v) => Poll::Ready(v),
				Poll::Pending => Poll::Pending,
			},
		}
	}
}
