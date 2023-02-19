use std::collections::{hash_map, HashMap};
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use log::{debug, error, trace, warn};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{sleep_until, Duration, Instant};

use tokio_util::codec::{self, Encoder, Decoder};

use bytes::Bytes;

use futures::future::{FusedFuture, FutureExt};
use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, Stream, StreamExt};

use serde::{Deserialize, Serialize};

use crate::bincode_codec::{BincodeCodec, DefaultBincodeCodec};
use crate::diag;
use crate::segments::Id;

type RequestId = u64;

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcMessage {
	#[serde(rename = "p")]
	ProgressPush(f64),
	#[serde(rename = "d")]
	DiagnosticsLog(diag::Level, String, String),
	#[serde(rename = "c")]
	StreamedChunk(StdResult<Bytes, String>),
}

impl fmt::Display for RpcMessage {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::ProgressPush(v) => write!(f, "ProgressPush({:?})", v),
			Self::DiagnosticsLog(level, subsystem, message) => write!(
				f,
				"DiagnosticsLog({:?}, {:?}, {:?})",
				level, subsystem, message
			),
			Self::StreamedChunk(result) => match result {
				Ok(data) => write!(f, "StreamedChunk(Ok(.. {} bytes ..))", data.len()),
				Err(e) => write!(f, "StreamedChunk(Err({:?}))", e),
			},
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRequest {
	#[serde(rename = "r")]
	RetrieveObject { id: Id },
	#[serde(rename = "s")]
	StreamObjects { ids: Vec<Id> },
	#[serde(rename = "i")]
	ContainsObject { id: Id },
	#[serde(rename = "f")]
	FindMissingObjects { ids: Vec<Id> },
	#[serde(rename = "c")]
	GetRepositoryConfigKey { key: String },
	#[serde(rename = "C")]
	CheckRepository,
}

impl fmt::Display for RpcRequest {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::FindMissingObjects { ids } => {
				write!(f, "FindMissingObjects {{ ids: .. {} ids .. }}", ids.len())
			}
			Self::StreamObjects { ids } => {
				write!(f, "StreamObjects {{ ids: .. {} ids .. }}", ids.len())
			}
			other => fmt::Debug::fmt(other, f),
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResponse {
	#[serde(rename = "D")]
	DataReply(Bytes),
	#[serde(rename = "b")]
	BoolReply(bool),
	#[serde(rename = "i")]
	IdListReply(Vec<Id>),
	#[serde(rename = "!")]
	Error(String),
	#[serde(rename = "-")]
	Nil,
	#[serde(rename = "s")]
	Success,
}

impl fmt::Display for RpcResponse {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::DataReply(data) => {
				write!(f, "DataReply(.. {} bytes ..)", data.len())
			}
			Self::IdListReply(ids) => {
				write!(f, "IdListReply(.. {} ids ..)", ids.len())
			}
			other => fmt::Debug::fmt(other, f),
		}
	}
}

impl RpcResponse {
	pub fn result(self) -> Result<RpcResponse> {
		match self {
			Self::Error(msg) => Err(Error::Remote(msg)),
			other => Ok(other),
		}
	}
}

impl<E: std::error::Error> From<StdResult<Bytes, E>> for RpcResponse {
	fn from(other: StdResult<Bytes, E>) -> Self {
		match other {
			Ok(v) => Self::DataReply(v),
			Err(e) => Self::Error(e.to_string()),
		}
	}
}

impl<E: std::error::Error> From<StdResult<Option<String>, E>> for RpcResponse {
	fn from(other: StdResult<Option<String>, E>) -> Self {
		match other {
			Ok(Some(v)) => Self::DataReply(v.into()),
			Ok(None) => Self::Nil,
			Err(e) => Self::Error(e.to_string()),
		}
	}
}

impl<E: std::error::Error> From<StdResult<bool, E>> for RpcResponse {
	fn from(other: StdResult<bool, E>) -> Self {
		match other {
			Ok(v) => Self::BoolReply(v),
			Err(e) => Self::Error(e.to_string()),
		}
	}
}

impl<E: std::error::Error> From<StdResult<Vec<Id>, E>> for RpcResponse {
	fn from(other: StdResult<Vec<Id>, E>) -> Self {
		match other {
			Ok(ids) => Self::IdListReply(ids),
			Err(e) => Self::Error(e.to_string()),
		}
	}
}

impl<E: std::error::Error> From<StdResult<(), E>> for RpcResponse {
	fn from(other: StdResult<(), E>) -> Self {
		match other {
			Ok(()) => Self::Success,
			Err(e) => Self::Error(e.to_string()),
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcItem {
	#[serde(rename = ",")]
	Message {
		in_reply_to: RequestId,
		payload: RpcMessage,
	},
	#[serde(rename = "?")]
	Request { id: RequestId, payload: RpcRequest },
	#[serde(rename = ".")]
	Response { id: RequestId, payload: RpcResponse },
	#[serde(rename = "h")]
	Heartbeat,
	#[serde(rename = "G")]
	Goodbye,
	#[serde(rename = "H")]
	Hello,
}

impl fmt::Display for RpcItem {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Message {
				in_reply_to,
				payload,
			} => write!(
				f,
				"Message {{ in_reply_to: {:?}, payload: {} }}",
				in_reply_to, payload
			),
			Self::Request { id, payload } => {
				write!(f, "Request {{ id: {:?}, payload: {} }}", id, payload)
			}
			Self::Response { id, payload } => {
				write!(f, "Response {{ id: {:?}, payload: {} }}", id, payload)
			}
			other => fmt::Debug::fmt(other, f),
		}
	}
}

#[derive(Debug)]
pub enum Error {
	LostWorker,
	Communication(io::Error),
	Remote(String),
	UnexpectedResponse(RpcResponse),
}

impl From<Error> for io::Error {
	fn from(other: Error) -> Self {
		match other {
			Error::LostWorker => io::Error::new(io::ErrorKind::BrokenPipe, other),
			Error::Remote(_) => io::Error::new(io::ErrorKind::Other, other),
			Error::Communication(e) => e,
			Error::UnexpectedResponse(_) => io::Error::new(io::ErrorKind::InvalidData, other),
		}
	}
}

impl From<io::Error> for Error {
	fn from(other: io::Error) -> Self {
		match other.get_ref().and_then(|x| x.downcast_ref::<Self>()) {
			Some(_) => {
				let inner = other.into_inner().unwrap();
				*inner.downcast().unwrap()
			}
			None => Self::Communication(other),
		}
	}
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::LostWorker => write!(f, "lost rpc worker or internal worker error"),
			Self::Communication(e) => write!(f, "communication error: {}", e),
			Self::Remote(msg) => write!(f, "remote error: {}", msg),
			Self::UnexpectedResponse(resp) => write!(f, "unexpected response received: {:?}", resp),
		}
	}
}

impl std::error::Error for Error {}

pub(super) type Result<T> = StdResult<T, Error>;
pub(super) type RpcWorkerCommand = (
	RpcRequest,
	Option<mpsc::Sender<RpcMessage>>,
	oneshot::Sender<io::Result<RpcResponse>>,
);

pub(super) enum CompletionStreamItem<M, R> {
	Data(M),
	Completed(R),
	Crashed,
}

pin_project_lite::pin_project! {
	pub(super) struct CompletionStream<M, R> {
		#[pin]
		stream: mpsc::Receiver<M>,
		#[pin]
		completion: futures::future::Fuse<oneshot::Receiver<R>>,
	}
}

impl CompletionStream<RpcMessage, io::Result<RpcResponse>> {
	pub(super) async fn rpc_call(
		ch: &mpsc::Sender<RpcWorkerCommand>,
		req: RpcRequest,
		depth: usize,
	) -> Result<Self> {
		let ((msg_tx, response_tx), result) = Self::channel(depth);
		match ch.send((req, Some(msg_tx), response_tx)).await {
			Ok(_) => (),
			Err(_) => return Err(Error::LostWorker),
		};
		Ok(result)
	}
}

impl<M, R> CompletionStream<M, R> {
	pub(super) fn wrap(msg_rx: mpsc::Receiver<M>, res_rx: oneshot::Receiver<R>) -> Self {
		Self {
			stream: msg_rx,
			completion: res_rx.fuse(),
		}
	}

	fn channel(depth: usize) -> ((mpsc::Sender<M>, oneshot::Sender<R>), Self) {
		let (msg_tx, msg_rx) = mpsc::channel(depth);
		let (res_tx, res_rx) = oneshot::channel();
		((msg_tx, res_tx), Self::wrap(msg_rx, res_rx))
	}
}

impl<M, R> Stream for CompletionStream<M, R> {
	type Item = CompletionStreamItem<M, R>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();
		if this.completion.is_terminated() {
			return Poll::Ready(None);
		}
		loop {
			match this.stream.as_mut().poll_recv(cx) {
				// if we get data, return it
				Poll::Ready(Some(msg)) => return Poll::Ready(Some(Self::Item::Data(msg))),
				// if we have to block, block
				Poll::Pending => return Poll::Pending,
				// if this is the eof, we have to poll on the oneshot
				Poll::Ready(None) => break,
			}
		}
		match this.completion.poll(cx) {
			Poll::Ready(Ok(v)) => Poll::Ready(Some(Self::Item::Completed(v))),
			Poll::Ready(Err(_)) => Poll::Ready(Some(Self::Item::Crashed)),
			Poll::Pending => Poll::Pending,
		}
	}
}

pub enum MessageSendError {
	AlreadyComplete,
	Other(Error),
}

pub enum TryMessageSendError {
	AlreadyComplete,
	Full(RpcMessage),
	Other(Error),
}

struct RequestContextInner {
	msg_ch: mpsc::Sender<(RpcMessage, oneshot::Sender<io::Result<()>>)>,
	reply_ch: oneshot::Sender<(RpcResponse, oneshot::Sender<io::Result<()>>)>,
}

struct RequestContextShared {
	// if this is None, reply() has been called
	inner: Option<RequestContextInner>,
}

pub struct MessageSender {
	shared: Weak<RwLock<RequestContextShared>>,
}

impl MessageSender {
	pub(super) async fn send(&self, payload: RpcMessage) -> StdResult<(), MessageSendError> {
		let ptr = match self.shared.upgrade() {
			Some(ptr) => ptr,
			_ => return Err(MessageSendError::AlreadyComplete),
		};
		let lock = ptr.read().await;
		let msg_ch = match lock.inner {
			Some(RequestContextInner { ref msg_ch, .. }) => msg_ch,
			None => return Err(MessageSendError::AlreadyComplete),
		};
		let (tx, rx) = oneshot::channel();
		match msg_ch.send((payload, tx)).await {
			Ok(_) => (),
			Err(_) => return Err(MessageSendError::Other(Error::LostWorker)),
		};
		match rx.await {
			Ok(Ok(())) => Ok(()),
			Ok(Err(e)) => Err(MessageSendError::Other(Error::Communication(e))),
			Err(_) => Err(MessageSendError::Other(Error::LostWorker)),
		}
	}

	pub(super) fn try_send(&self, payload: RpcMessage) -> StdResult<(), TryMessageSendError> {
		let ptr = match self.shared.upgrade() {
			Some(ptr) => ptr,
			_ => return Err(TryMessageSendError::AlreadyComplete),
		};
		let lock = match ptr.try_read() {
			Ok(lock) => lock,
			// the only way we can fail to acquire the lock is if it's
			// currently being locked by the reply(), in order to shut down
			// the thing, so returning the AlreadyComplete error is
			// appropriate.
			Err(_) => return Err(TryMessageSendError::AlreadyComplete),
		};
		let msg_ch = match lock.inner {
			Some(RequestContextInner { ref msg_ch, .. }) => msg_ch,
			None => return Err(TryMessageSendError::AlreadyComplete),
		};
		let (tx, _) = oneshot::channel();
		match msg_ch.try_send((payload, tx)) {
			Ok(_) => Ok(()),
			Err(mpsc::error::TrySendError::Full((payload, _))) => {
				Err(TryMessageSendError::Full(payload))
			}
			Err(mpsc::error::TrySendError::Closed(_)) => {
				Err(TryMessageSendError::Other(Error::LostWorker))
			}
		}
	}
}

pub struct RequestContext {
	shared: Arc<RwLock<RequestContextShared>>,
}

impl RequestContext {
	fn new(
		msg_ch: mpsc::Sender<(RpcMessage, oneshot::Sender<io::Result<()>>)>,
		reply_ch: oneshot::Sender<(RpcResponse, oneshot::Sender<io::Result<()>>)>,
	) -> Self {
		Self {
			shared: Arc::new(RwLock::new(RequestContextShared {
				inner: Some(RequestContextInner { msg_ch, reply_ch }),
			})),
		}
	}

	pub fn message_sender(&self) -> MessageSender {
		MessageSender {
			shared: Arc::downgrade(&self.shared),
		}
	}

	pub async fn send_message(&self, payload: RpcMessage) -> Result<()> {
		let lock = self.shared.read().await;
		let msg_ch = &lock
			.inner
			.as_ref()
			.expect("send_message cannot be called after reply()")
			.msg_ch;
		let (tx, rx) = oneshot::channel();
		match msg_ch.send((payload, tx)).await {
			Ok(_) => (),
			Err(_) => return Err(Error::LostWorker),
		};
		match rx.await {
			Ok(Ok(())) => Ok(()),
			Ok(Err(e)) => Err(Error::Communication(e)),
			Err(_) => Err(Error::LostWorker),
		}
	}

	pub async fn reply(self, payload: RpcResponse) -> Result<()> {
		let reply_ch = {
			let mut lock = self.shared.write().await;
			lock.inner
				.take()
				.expect("reply can only be called once")
				.reply_ch
		};
		let (tx, rx) = oneshot::channel();
		match reply_ch.send((payload, tx)) {
			Ok(_) => (),
			Err(_) => return Err(Error::LostWorker),
		};
		match rx.await {
			Ok(Ok(())) => Ok(()),
			Ok(Err(e)) => Err(Error::Communication(e)),
			Err(_) => Err(Error::LostWorker),
		}
	}
}

type WorkerStream = CompletionStream<
	(RpcMessage, oneshot::Sender<io::Result<()>>),
	(RpcResponse, oneshot::Sender<io::Result<()>>),
>;

enum RequestWorkerItem {
	Message(RequestId, RpcMessage, oneshot::Sender<io::Result<()>>),
	Completed(RequestId, RpcResponse, oneshot::Sender<io::Result<()>>),
	Crashed(RequestId),
}

struct RequestWorkers {
	slots: Vec<(RequestId, WorkerStream)>,
}

impl RequestWorkers {
	fn new() -> Self {
		Self { slots: Vec::new() }
	}

	fn add(&mut self, request_id: RequestId, stream: WorkerStream) {
		self.slots.push((request_id, stream))
	}
}

impl Stream for RequestWorkers {
	type Item = RequestWorkerItem;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		for (i, (req_id, ch)) in self.slots.iter_mut().enumerate() {
			match Pin::new(ch).poll_next(cx) {
				Poll::Ready(Some(CompletionStreamItem::Completed((msg, ch)))) => {
					let req_id = *req_id;
					self.slots.remove(i);
					return Poll::Ready(Some(Self::Item::Completed(req_id, msg, ch)));
				}
				Poll::Ready(Some(CompletionStreamItem::Data((msg, ch)))) => {
					return Poll::Ready(Some(Self::Item::Message(*req_id, msg, ch)));
				}
				Poll::Ready(Some(CompletionStreamItem::Crashed)) | Poll::Ready(None) => {
					let req_id = *req_id;
					self.slots.remove(i);
					return Poll::Ready(Some(Self::Item::Crashed(req_id)));
				}
				Poll::Pending => continue,
			}
		}
		Poll::Pending
	}
}

pub(super) type RpcWorkerMessage = (RequestContext, RpcRequest);

static WORKER_NUM_GEN: AtomicUsize = AtomicUsize::new(1);

pub(super) struct RpcWorkerConfig {
	request_queue_size: usize,
	message_queue_size: usize,
	name: String,
}

impl RpcWorkerConfig {
	pub(super) fn with_name_prefix(prefix: &str) -> Self {
		let num = WORKER_NUM_GEN.fetch_add(1, Ordering::SeqCst);
		Self {
			request_queue_size: 16,
			message_queue_size: 16,
			name: format!("{}-{:02}", prefix, num),
		}
	}

	#[allow(dead_code)]
	pub(super) fn set_request_queue_size(&mut self, new: usize) -> &mut Self {
		self.request_queue_size = new;
		self
	}

	#[allow(dead_code)]
	pub(super) fn set_message_queue_size(&mut self, new: usize) -> &mut Self {
		self.message_queue_size = new;
		self
	}

	pub(super) fn spawn<T: AsyncRead + AsyncWrite + Send + 'static>(
		self,
		channel: T,
	) -> (
		tokio::task::JoinHandle<()>,
		mpsc::Sender<RpcWorkerCommand>,
		mpsc::Receiver<RpcWorkerMessage>,
	) {
		let (tx, rx) = codec::Framed::new(channel, BincodeCodec::new()).split();
		let (request_tx, request_rx) = mpsc::channel(self.request_queue_size);
		let (message_tx, message_rx) = mpsc::channel(self.message_queue_size);
		let worker = RpcWorker::<T, std::convert::Infallible, RpcItem, DefaultBincodeCodec<RpcItem>, RpcItem, DefaultBincodeCodec<RpcItem>> {
			tx,
			rx,
			next_id: 0,
			request_rx,
			message_tx,
			response_handlers: HashMap::new(),
			message_handlers: HashMap::new(),
			request_workers: RequestWorkers::new(),
			name: self.name,
		};
		let join_handle = tokio::spawn(worker.run());
		(join_handle, request_tx, message_rx)
	}

	pub(super) async fn spawn_borg<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
		self,
		channel: T,
		repository_path: String,
	) -> io::Result<(
		tokio::task::JoinHandle<()>,
		mpsc::Sender<RpcWorkerCommand>,
		mpsc::Receiver<RpcWorkerMessage>,
	)> {
		use super::borg::{BorgRpcRequest, BorgRpcResponse, borg_open};
		use crate::rmp_codec::AsymMpCodec;
		let mut ch = codec::Framed::new(channel, AsymMpCodec::new());
		let (request_tx, request_rx) = mpsc::channel(self.request_queue_size);
		let (message_tx, message_rx) = mpsc::channel(self.message_queue_size);

		// before we spawn the worker, we have to open the repository
		borg_open(&mut ch, repository_path).await?;

		let (tx, rx) = ch.split();

		let worker = RpcWorker::<T, RpcInterfaceError, BorgRpcRequest, AsymMpCodec<BorgRpcRequest, BorgRpcResponse>, BorgRpcResponse, AsymMpCodec<BorgRpcRequest, BorgRpcResponse>> {
			tx,
			rx,
			next_id: 0,
			request_rx,
			message_tx,
			response_handlers: HashMap::new(),
			message_handlers: HashMap::new(),
			request_workers: RequestWorkers::new(),
			name: self.name,
		};
		let join_handle = tokio::spawn(worker.run());
		Ok((join_handle, request_tx, message_rx))
	}
}

#[derive(Clone)]
enum ErrorGenerator {
	HandshakeError(Arc<io::Error>),
	SendError(Arc<io::Error>),
	ReceiveError(Arc<io::Error>),
	LocalShutdown,
	RemoteShutdown,
}

impl ErrorGenerator {
	fn send(other: io::Error) -> Self {
		Self::SendError(Arc::new(other))
	}

	fn receive(other: io::Error) -> Self {
		Self::ReceiveError(Arc::new(other))
	}

	fn handshake(other: io::Error) -> Self {
		Self::HandshakeError(Arc::new(other))
	}
}

impl From<&ErrorGenerator> for io::Error {
	fn from(other: &ErrorGenerator) -> Self {
		match other {
			ErrorGenerator::HandshakeError(e) => {
				Self::new(e.kind(), format!("handshake error: {}", e))
			}
			ErrorGenerator::LocalShutdown => Self::new(io::ErrorKind::BrokenPipe, "local shutdown"),
			ErrorGenerator::RemoteShutdown => {
				Self::new(io::ErrorKind::ConnectionReset, "remote shutdown")
			}
			ErrorGenerator::SendError(e) => Self::new(e.kind(), format!("send error: {}", e)),
			ErrorGenerator::ReceiveError(e) => Self::new(e.kind(), format!("receive error: {}", e)),
		}
	}
}

pub(super) enum RpcInterfaceError {
	UnsupportedIgnore(RpcItem),
	UnsupportedFail(RpcItem),
}

macro_rules! handle_tx {
	(($name:expr, $obj:expr) => $tx:expr => {
		Ok($okv:pat_param) => $ok:expr,
		Err($errv:pat_param) => $err:expr => break,
	}) => {
		trace!("{}: tx: {}", $name, $obj);
		match $obj.try_into().map_err(|x: TE| x.into()) {
			Ok(v) => {
				match $tx.send(v).await.map_err(ErrorGenerator::send) {
					Ok($okv) => $ok,
					Err(e) => {
						{
							let $errv = &e;
							$err;
						}
						break Err(e);
					}
				}
			},
			Err(RpcInterfaceError::UnsupportedIgnore(v)) => {
				trace!("{}: transmission of {} not supported, dropping", $name, v);
				$ok
			},
			Err(RpcInterfaceError::UnsupportedFail(v)) => {
				let e = ErrorGenerator::send(io::Error::new(io::ErrorKind::InvalidInput, format!("{} cannot be sent over transport", v)));
				{
					let $errv = &e;
					$err;
				}
				break Err(e);
			}
		}
	};
}

macro_rules! fulfill_reply {
	($ch:expr, $v:expr) => {
		if let Some(ch) = $ch {
			let _: StdResult<_, _> = ch.send($v);
		}
	};
}

impl From<std::convert::Infallible> for RpcInterfaceError {
	fn from(_other: std::convert::Infallible) -> Self {
		unreachable!();
	}
}

struct RpcWorker<T: AsyncRead + AsyncWrite, TE: Into<RpcInterfaceError>, TX: TryFrom<RpcItem, Error = TE>, ENC: Encoder<TX, Error = io::Error>, RX: Into<RpcItem>, DEC: Decoder<Item = RX, Error = io::Error>> {
	tx: SplitSink<codec::Framed<T, ENC>, TX>,
	rx: SplitStream<codec::Framed<T, DEC>>,
	next_id: RequestId,
	request_rx: mpsc::Receiver<RpcWorkerCommand>,
	message_tx: mpsc::Sender<RpcWorkerMessage>,
	response_handlers: HashMap<RequestId, oneshot::Sender<io::Result<RpcResponse>>>,
	message_handlers: HashMap<RequestId, mpsc::Sender<RpcMessage>>,
	request_workers: RequestWorkers,
	name: String,
}

impl<T: AsyncRead + AsyncWrite, TE: Into<RpcInterfaceError>, TX: TryFrom<RpcItem, Error = TE>, ENC: Encoder<TX, Error = io::Error>, RX: Into<RpcItem>, DEC: Decoder<Item = RX, Error = io::Error>> RpcWorker<T, TE, TX, ENC, RX, DEC> {
	fn generate_next_id(&mut self) -> RequestId {
		loop {
			let id = self.next_id;
			// using a non-1 number here for easier visual distinguishing of request IDs
			// using a prime to still generate all possible u64 numbers before truly reusing IDs
			self.next_id = self.next_id.wrapping_add(99971);
			if !self.response_handlers.contains_key(&id) && !self.message_handlers.contains_key(&id)
			{
				return id;
			}
		}
	}

	fn tx_deadline() -> Instant {
		Instant::now() + Duration::new(45, 0)
	}

	fn rx_deadline() -> Instant {
		Instant::now() + Duration::new(120, 0)
	}

	fn disabled_tx_deadline() -> Instant {
		Instant::now() + Duration::new(3600, 0)
	}

	fn disabled_rx_deadline() -> Instant {
		Instant::now() + Duration::new(7200, 0)
	}

	fn drain_with_error(mut self, e: &ErrorGenerator) {
		match e {
			ErrorGenerator::LocalShutdown => {
				debug!("{}: shutting down (at local request)", self.name)
			}
			ErrorGenerator::RemoteShutdown => {
				debug!("{}: shutting down (at remote request)", self.name)
			}
			other => {
				let err = io::Error::from(other);
				error!("{}: shutting down because of error: {}", self.name, err);
			}
		};
		self.request_rx.close();
		drop(self.message_tx);
		drop(self.request_workers);
		self.message_handlers.clear();
		for (_, handler) in self.response_handlers.drain() {
			fulfill_reply!(Some(handler), Err(e.into()));
		}
		// we ignore permits
		while let Ok(req) = self.request_rx.try_recv() {
			match req {
				(_, _, response_handler) => {
					let _: StdResult<_, _> = response_handler.send(Err(e.into()));
				}
			}
		}
	}

	async fn run(mut self) {
		// Protocol layout:
		// 1. Exchange hellos
		// 2. Exchange requests/messages/responses
		// 3. Send goodbye

		// send heartbeat immediately after receiving the handshake
		let tx_timeout = sleep_until(Instant::now());
		tokio::pin!(tx_timeout);
		match RpcItem::Hello.try_into() {
			Ok(hello) => {
				debug!("{}: sending hello", self.name);
				match self.tx.send(hello).await {
					Ok(()) => (),
					Err(e) => {
						self.drain_with_error(&ErrorGenerator::handshake(e));
						return;
					}
				};
				match self.rx.next().await.map(|x| x.map(|x| x.into())) {
					Some(Ok(RpcItem::Hello)) => (),
					None => {
						let e = io::Error::new(io::ErrorKind::UnexpectedEof, "no Hello received");
						self.drain_with_error(&ErrorGenerator::handshake(e));
						return;
					}
					Some(Ok(_)) => {
						let e = io::Error::new(
							io::ErrorKind::InvalidData,
							"unexpected message during handshake",
						);
						self.drain_with_error(&ErrorGenerator::handshake(e));
						return;
					}
					Some(Err(e)) => {
						self.drain_with_error(&ErrorGenerator::handshake(e));
						return;
					}
				};
			}
			Err(_) => {
				debug!("{}: hello not supported by transport, skipping", self.name);
			}
		}
		let rx_timeout = sleep_until(Self::rx_deadline());
		tokio::pin!(rx_timeout);
		let mut heartbeats_supported = true;

		debug!("{}: handshake complete", self.name);
		// Hellos are now exchanged successfully, we can enter the main item exchange
		let result: StdResult<(), ErrorGenerator> = loop {
			tokio::select! {
				// tx timeout: every 45s of silence, we send a Heartbeat
				_ = &mut tx_timeout, if heartbeats_supported => {
					match RpcItem::Heartbeat.try_into() {
						Ok(heartbeat) => {
							match self.tx.send(heartbeat).await {
								Ok(_) => (),
								Err(e) => break Err(ErrorGenerator::send(e)),
							};
							tx_timeout.as_mut().reset(Self::tx_deadline());
						}
						// if heartbeats are not supported, we disable all timeouts
						Err(_) => {
							debug!("{}: heartbeats not supported, disabling all timeouts", self.name);
							heartbeats_supported = false;
							tx_timeout.as_mut().reset(Self::disabled_tx_deadline());
							rx_timeout.as_mut().reset(Self::disabled_rx_deadline());
						}
					}
				}

				// rx timeout: every 120s at least we expect to hear from our partner
				// if we do not, that's an rx timeout, which is fatal
				// NOTE: the rx_timeout is set into the very far future by the tx_timeout handler if heartbaets are not supported
				_ = &mut rx_timeout, if heartbeats_supported => {
					break Err(ErrorGenerator::receive(io::Error::new(io::ErrorKind::TimedOut, "receive timeout elapsed")));
				}

				// received item (message) from peer
				item = self.rx.next() => {
					let item = item.map(|x| x.map(|x| x.into()));
					// advance receive timeout -- as long as data is pouring in, we don't care about round-trip times
					rx_timeout.as_mut().reset(Self::rx_deadline());
					if let Some(Ok(item)) = item.as_ref() {
						trace!("{} rx: {}", self.name, item);
					}
					match item {
						// push-style message, potentially related to an ongoing request
						Some(Ok(RpcItem::Message{in_reply_to, payload})) => {
							match self.message_handlers.entry(in_reply_to) {
								// no recipient, drop
								hash_map::Entry::Vacant(_) => {
									// this is debug, because it can be triggered by noisy servers
									debug!("{} received message {} for request id {}, but no receiver exists", self.name, payload, in_reply_to);
								},
								hash_map::Entry::Occupied(o) => {
									match o.get().send(payload).await {
										Ok(_) => (),
										Err(mpsc::error::SendError(payload)) => {
											// this is warn, because in contrast to the above, its not (easily) possible for noisy servers to trigger this: we remove message listeners when their task completes... so this requires a rather intricate race condition
											warn!("{} received message {} for request id {}, but the receiver has terminated", self.name, payload, in_reply_to);
											o.remove();
										}
									}
								}
							}
						}

						// incoming request: send request down the chute and keep track of it to make sure we send a reply always, even if the corresponding task dies)
						Some(Ok(RpcItem::Request{id, payload})) => {
							let ((msg_tx, res_tx), ch) = CompletionStream::<(RpcMessage, oneshot::Sender<io::Result<()>>), (RpcResponse, oneshot::Sender<io::Result<()>>)>::channel(1);
							let ctx = RequestContext::new(
								msg_tx,
								res_tx,
							);
							debug!("{}: starting request {}: {}", self.name, id, payload);
							match self.message_tx.send((ctx, payload)).await {
								Ok(_) => {
									// successfully sent *somewhere*, so we need to poll it
									self.request_workers.add(id, ch);
								}
								Err(mpsc::error::SendError((_, _))) => {
									// there is nothing there handling requests -> return error
									debug!("{} cannot start any requests", self.name);
									handle_tx! {
										(self.name, RpcItem::Response{id, payload: RpcResponse::Error("request refused".into())}) => self.tx => {
											Ok(()) => (),
											Err(_) => () => break,
										}
									}
									tx_timeout.as_mut().reset(Self::tx_deadline());
								}
							};
						}

						// incoming response: route it to the corresponding task.
						Some(Ok(RpcItem::Response{id, payload})) => {
							self.message_handlers.remove(&id);
							match self.response_handlers.remove(&id) {
								Some(v) => {
									debug!("{}: received response to known request {}", self.name, id);
									// if the task died in the meantime, we don't care.
									let _: StdResult<_, _> = v.send(Ok(payload));
								}
								// unknown id, ignore
								None => (),
							}
						}

						// ignore incoming heartbeats (we advanced the rx timeout above already)
						Some(Ok(RpcItem::Heartbeat)) => (),

						// request to shutdown, we honour that
						Some(Ok(RpcItem::Goodbye)) => break Err(ErrorGenerator::RemoteShutdown),

						// protocol violation, but don't care
						Some(Ok(RpcItem::Hello)) => (),

						// receive error, exit with error
						Some(Err(e)) => break Err(ErrorGenerator::receive(e)),

						// eof before goodbye, exit with error
						None => break Err(ErrorGenerator::receive(io::Error::new(io::ErrorKind::UnexpectedEof, "unclean closure of connection"))),
					}
				},

				// one of our request workers (see above) finished
				item = self.request_workers.next() => {
					// unwrap: request_workers is an inifinte stream
					let item = item.unwrap();
					let (item, reply_ch) = match item {
						RequestWorkerItem::Crashed(req_id) => {
							debug!("{}: handler for request {} exited unexpectedly", self.name, req_id);
							(RpcItem::Response{id: req_id, payload: RpcResponse::Error("internal server error".into())}, None)
						}
						RequestWorkerItem::Completed(req_id, resp, ch) => {
							debug!("{}: request {} completed with result {}", self.name, req_id, resp);
							(RpcItem::Response{id: req_id, payload: resp}, Some(ch))
						}
						RequestWorkerItem::Message(req_id, msg, ch) => {
							(RpcItem::Message{in_reply_to: req_id, payload: msg}, Some(ch))
						}
					};
					handle_tx! {
						(self.name, item) => self.tx => {
							Ok(()) => fulfill_reply!(reply_ch, Ok(())),
							Err(e) => fulfill_reply!(reply_ch, Err(e.into())) => break,
						}
					}
					tx_timeout.as_mut().reset(Self::tx_deadline());
				}

				// internal request to do something
				request = self.request_rx.recv() => {
					match request {
						// send a request to the peer
						Some((payload, message_handler, response_handler)) => {
							// we are responsible for ID bookkeeping, not the requester; this way, we don't need another synchronization point for picking IDs and we can avoid re-using IDs which are currently in-flight'
							let id = self.generate_next_id();
							let item = RpcItem::Request{id, payload};
							// register the handlers first, so that they're taken care of during drain if anything goes wrong
							self.response_handlers.insert(id, response_handler);
							if let Some(message_handler) = message_handler {
								self.message_handlers.insert(id, message_handler);
							}
							handle_tx! {
								(self.name, item) => self.tx => {
									Ok(()) => (),
									Err(_) => () => break,
								}
							}
							tx_timeout.as_mut().reset(Self::tx_deadline());
						},
						None => break Ok(()),
					}
				},
			}
		};
		let errgen = match result {
			Ok(()) => {
				// we try to send a Goodbye message, and if we don't succeed, that changes the error state'
				match RpcItem::Goodbye.try_into() {
					Ok(v) => {
						self.tx
							.send(v)
							.await
							.map_err(ErrorGenerator::send)
					}
					// if unsupported, ignore
					Err(_) => Ok(()),
				}
							.err()
							.unwrap_or(ErrorGenerator::LocalShutdown)
			}
			Err(e) => e,
		};
		self.drain_with_error(&errgen);
	}
}
