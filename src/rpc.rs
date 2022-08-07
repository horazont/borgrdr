use std::collections::{hash_map, HashMap, VecDeque};
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

use tokio_util::codec;

use bytes::Bytes;

use futures::future::{FusedFuture, FutureExt};
use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, Stream, StreamExt};

use serde::{Deserialize, Serialize};

use super::bincode_codec::{BincodeCodec, DefaultBincodeCodec};
use super::diag;
use super::diag::{DiagnosticsSink, Progress};
use super::segments::Id;
use super::store::ObjectStore;

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
	#[serde(rename = "v")]
	RepoKeyValueReply(Option<String>),
	#[serde(rename = "!")]
	Error(String),
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
			Ok(v) => Self::RepoKeyValueReply(v),
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

type Result<T> = StdResult<T, Error>;
type RpcWorkerCommand = (
	RpcRequest,
	Option<mpsc::Sender<RpcMessage>>,
	oneshot::Sender<io::Result<RpcResponse>>,
);

enum CompletionStreamItem<M, R> {
	Data(M),
	Completed(R),
	Crashed,
}

pin_project_lite::pin_project! {
	struct CompletionStream<M, R> {
		#[pin]
		stream: mpsc::Receiver<M>,
		#[pin]
		completion: futures::future::Fuse<oneshot::Receiver<R>>,
	}
}

impl CompletionStream<RpcMessage, io::Result<RpcResponse>> {
	async fn rpc_call(
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
	fn wrap(msg_rx: mpsc::Receiver<M>, res_rx: oneshot::Receiver<R>) -> Self {
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
	async fn send(&self, payload: RpcMessage) -> StdResult<(), MessageSendError> {
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

	fn try_send(&self, payload: RpcMessage) -> StdResult<(), TryMessageSendError> {
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

type RpcWorkerMessage = (RequestContext, RpcRequest);

static WORKER_NUM_GEN: AtomicUsize = AtomicUsize::new(1);

struct RpcWorkerConfig {
	request_queue_size: usize,
	message_queue_size: usize,
	name: String,
}

impl RpcWorkerConfig {
	fn with_name_prefix(prefix: &str) -> Self {
		let num = WORKER_NUM_GEN.fetch_add(1, Ordering::SeqCst);
		Self {
			request_queue_size: 16,
			message_queue_size: 16,
			name: format!("{}-{:02}", prefix, num),
		}
	}

	#[allow(dead_code)]
	fn set_request_queue_size(&mut self, new: usize) -> &mut Self {
		self.request_queue_size = new;
		self
	}

	#[allow(dead_code)]
	fn set_message_queue_size(&mut self, new: usize) -> &mut Self {
		self.message_queue_size = new;
		self
	}

	fn spawn<T: AsyncRead + AsyncWrite + Send + 'static>(
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
		let worker = RpcWorker {
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

macro_rules! handle_tx {
	(($name:expr, $obj:expr) => $tx:expr => {
		Ok($okv:pat_param) => $ok:expr,
		Err($errv:pat_param) => $err:expr => break,
	}) => {
		trace!("{}: tx: {}", $name, $obj);
		match $tx.send($obj).await.map_err(ErrorGenerator::send) {
			Ok($okv) => $ok,
			Err(e) => {
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

struct RpcWorker<T: AsyncRead + AsyncWrite> {
	tx: SplitSink<codec::Framed<T, DefaultBincodeCodec<RpcItem>>, RpcItem>,
	rx: SplitStream<codec::Framed<T, DefaultBincodeCodec<RpcItem>>>,
	next_id: RequestId,
	request_rx: mpsc::Receiver<RpcWorkerCommand>,
	message_tx: mpsc::Sender<RpcWorkerMessage>,
	response_handlers: HashMap<RequestId, oneshot::Sender<io::Result<RpcResponse>>>,
	message_handlers: HashMap<RequestId, mpsc::Sender<RpcMessage>>,
	request_workers: RequestWorkers,
	name: String,
}

impl<T: AsyncRead + AsyncWrite> RpcWorker<T> {
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

		debug!("{}: sending hello", self.name);
		match self.tx.send(RpcItem::Hello).await {
			Ok(()) => (),
			Err(e) => {
				self.drain_with_error(&ErrorGenerator::handshake(e));
				return;
			}
		};
		// send heartbeat immediately after receiving the handshake
		let tx_timeout = sleep_until(Instant::now());
		tokio::pin!(tx_timeout);
		match self.rx.next().await {
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
		let rx_timeout = sleep_until(Self::rx_deadline());
		tokio::pin!(rx_timeout);

		debug!("{}: handshake complete", self.name);
		// Hellos are now exchanged successfully, we can enter the main item exchange
		let result: StdResult<(), ErrorGenerator> = loop {
			tokio::select! {
				// tx timeout: every 45s of silence, we send a Heartbeat
				_ = &mut tx_timeout => {
					match self.tx.send(RpcItem::Heartbeat).await {
						Ok(_) => (),
						Err(e) => break Err(ErrorGenerator::send(e)),
					};
					tx_timeout.as_mut().reset(Self::tx_deadline());
				}

				// rx timeout: every 120s at least we expect to hear from our partner
				// if we do not, that's an rx timeout, which is fatal
				_ = &mut rx_timeout => {
					break Err(ErrorGenerator::receive(io::Error::new(io::ErrorKind::TimedOut, "receive timeout elapsed")));
				}

				// received item (message) from peer
				item = self.rx.next() => {
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
				self.tx
					.send(RpcItem::Goodbye)
					.await
					.map_err(ErrorGenerator::send)
					.err()
					.unwrap_or(ErrorGenerator::LocalShutdown)
			}
			Err(e) => e,
		};
		self.drain_with_error(&errgen);
	}
}

macro_rules! match_rpc_response {
	($x:expr => {
		$($p:pat_param => $px:expr,)*
	}) => {
		match $x {
			$($p => $px,)*
			Ok(RpcResponse::Error(e)) => Err(Error::Remote(e)),
			Ok(other) => Err(Error::UnexpectedResponse(other)),
			Err(e) => Err(e),
		}
	}
}

pub struct RpcStoreClient {
	request_ch: mpsc::Sender<RpcWorkerCommand>,
}

impl RpcStoreClient {
	pub fn new<I: AsyncRead + AsyncWrite + Send + 'static>(inner: I) -> Self {
		let (_, ch_tx, _) = RpcWorkerConfig::with_name_prefix("client").spawn(inner);
		Self { request_ch: ch_tx }
	}

	async fn rpc_call(
		&self,
		req: RpcRequest,
		message_sink: Option<mpsc::Sender<RpcMessage>>,
	) -> Result<RpcResponse> {
		let (response_tx, response_rx) = oneshot::channel();
		match self.request_ch.send((req, message_sink, response_tx)).await {
			Ok(_) => (),
			Err(_) => return Err(Error::LostWorker),
		};
		match response_rx.await {
			Ok(Ok(RpcResponse::Error(remote_err))) => Err(Error::Remote(remote_err)),
			Ok(Ok(other)) => Ok(other),
			Ok(Err(e)) => Err(Error::Communication(e)),
			Err(_) => Err(Error::LostWorker),
		}
	}
}

#[async_trait::async_trait]
impl ObjectStore for RpcStoreClient {
	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes> {
		let id = id.as_ref().clone();
		match_rpc_response! {
			self.rpc_call(RpcRequest::RetrieveObject{id}, None).await => {
				Ok(RpcResponse::DataReply(data)) => Ok(data),
			}
		}
		.map_err(|x| x.into())
	}

	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool> {
		let id = id.as_ref().clone();
		match_rpc_response! {
			self.rpc_call(RpcRequest::ContainsObject{id}, None).await => {
				Ok(RpcResponse::BoolReply(data)) => Ok(data),
			}
		}
		.map_err(|x| x.into())
	}

	async fn find_missing_objects(&self, ids: Vec<Id>) -> io::Result<Vec<Id>> {
		match_rpc_response! {
			self.rpc_call(RpcRequest::FindMissingObjects{ids}, None).await => {
				Ok(RpcResponse::IdListReply(ids)) => Ok(ids),
			}
		}
		.map_err(|x| x.into())
	}

	async fn get_repository_config_key(&self, key: &str) -> io::Result<Option<String>> {
		let key = key.to_string();
		match_rpc_response! {
			self.rpc_call(RpcRequest::GetRepositoryConfigKey{key}, None).await => {
				Ok(RpcResponse::RepoKeyValueReply(data)) => Ok(data),
			}
		}
		.map_err(|x| x.into())
	}

	async fn check_all_segments(
		&self,
		mut progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()> {
		let call =
			CompletionStream::rpc_call(&self.request_ch, RpcRequest::CheckRepository, 2).await?;
		tokio::pin!(call);
		let result = loop {
			match futures::future::poll_fn(|cx| call.as_mut().poll_next(cx)).await {
				Some(CompletionStreamItem::Data(RpcMessage::ProgressPush(v))) => {
					progress.progress(Progress::Ratio(v));
				}
				Some(CompletionStreamItem::Data(RpcMessage::DiagnosticsLog(
					level,
					subsystem,
					message,
				))) => {
					progress.log(level, &subsystem, &message);
				}
				Some(CompletionStreamItem::Data(_)) => (),
				Some(CompletionStreamItem::Completed(result)) => {
					break result.map_err(|x| x.into());
				}
				Some(CompletionStreamItem::Crashed) => {
					break Err(Error::LostWorker);
				}
				None => unreachable!(),
			}
		};
		match_rpc_response! {
			result => {
				Ok(RpcResponse::Success) => Ok(()),
			}
		}
		.map_err(|x| x.into())
	}

	type ObjectStream = ObjectStream;

	fn stream_objects(&self, object_ids: Vec<Id>) -> io::Result<ObjectStream> {
		Ok(ObjectStream {
			backend: tokio_util::sync::PollSender::new(self.request_ch.clone()),
			src: object_ids,
			block_size: 128,
			curr_chunk: None,
		})
	}
}

pin_project_lite::pin_project! {
	pub struct ObjectStream {
		#[pin]
		backend: tokio_util::sync::PollSender<RpcWorkerCommand>,
		src: Vec<Id>,
		block_size: usize,
		#[pin]
		curr_chunk: Option<CompletionStream<RpcMessage, io::Result<RpcResponse>>>,
	}
}

impl Stream for ObjectStream {
	type Item = io::Result<Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		//println!("ObjectStream asked for more");
		let mut this = self.project();
		loop {
			if this.curr_chunk.is_none() {
				//println!("ObjectStream needs to request more");
				if this.src.len() == 0 {
					//println!("ObjectStream exhausted");
					return Poll::Ready(None);
				}

				match this.backend.as_mut().poll_reserve(cx) {
					Poll::Pending => return Poll::Pending,
					Poll::Ready(Err(_)) => return Poll::Ready(Some(Err(Error::LostWorker.into()))),
					Poll::Ready(Ok(())) => (),
				}

				// we can now prepare the sending :-)
				let size = (*this.block_size).min(this.src.len());
				let ids = this.src.drain(..size).collect();
				let (result_tx, result_rx) = oneshot::channel();
				let (msg_tx, msg_rx) = mpsc::channel(*this.block_size);
				match this.backend.send_item((
					RpcRequest::StreamObjects { ids },
					Some(msg_tx),
					result_tx,
				)) {
					Ok(()) => (),
					Err(_) => return Poll::Ready(Some(Err(Error::LostWorker.into()))),
				};

				*this.curr_chunk = Some(CompletionStream::wrap(msg_rx, result_rx));
			}

			//println!("ObjectStream asking inner stream");
			match this.curr_chunk.as_mut().as_pin_mut().unwrap().poll_next(cx) {
				// forward streamed data to user
				Poll::Ready(Some(CompletionStreamItem::Data(RpcMessage::StreamedChunk(data)))) => {
					return Poll::Ready(Some(data.map_err(Error::Remote).map_err(|x| x.into())))
				}
				// ignore unexpected messages
				Poll::Ready(Some(CompletionStreamItem::Data(_))) => (),
				// if completed ...
				Poll::Ready(Some(CompletionStreamItem::Completed(Ok(v)))) => {
					*this.curr_chunk = None;
					match v.result() {
						// ... with success, we try the next one
						Ok(_) => (),
						// ... with error, we return the error
						Err(e) => return Poll::Ready(Some(Err(e.into()))),
					}
				}
				// if completed with error (e.g. send error), we return the error
				Poll::Ready(Some(CompletionStreamItem::Completed(Err(e)))) => {
					*this.curr_chunk = None;
					return Poll::Ready(Some(Err(e)));
				}
				// if completed unexpectedly, continue and inject LostWorker error
				Poll::Ready(None) | Poll::Ready(Some(CompletionStreamItem::Crashed)) => {
					*this.curr_chunk = None;
					return Poll::Ready(Some(Err(Error::LostWorker.into())));
				}
				Poll::Pending => return Poll::Pending,
			}

			*this.curr_chunk = None;
		}
	}
}

struct BufferedSender {
	sink: MessageSender,
	buffer: VecDeque<RpcMessage>,
}

impl BufferedSender {
	pub fn wrap(inner: MessageSender) -> Self {
		// we need at least a capacity of 1, because we always first push and then pop to try the initial send even.
		Self {
			sink: inner,
			buffer: VecDeque::with_capacity(1),
		}
	}

	pub fn send(&mut self, v: RpcMessage) -> StdResult<(), MessageSendError> {
		self.buffer.push_back(v);
		while let Some(item) = self.buffer.pop_front() {
			match self.sink.try_send(item) {
				Ok(()) => (),
				Err(TryMessageSendError::AlreadyComplete) => {
					// this is fatal
					self.buffer.clear();
					self.buffer.shrink_to_fit();
					return Err(MessageSendError::AlreadyComplete);
				}
				Err(TryMessageSendError::Full(v)) => {
					self.buffer.push_front(v);
					return Ok(());
				}
				Err(TryMessageSendError::Other(e)) => {
					// also certainly fatal
					self.buffer.clear();
					self.buffer.shrink_to_fit();
					return Err(MessageSendError::Other(e));
				}
			}
		}
		Ok(())
	}

	pub async fn flush(&mut self) -> StdResult<(), MessageSendError> {
		let mut err = None;
		while let Some(item) = self.buffer.pop_front() {
			match self.sink.send(item).await {
				Ok(()) => (),
				Err(e) => {
					eprintln!("error during flush, not retriable, lost");
					err = Some(e);
				}
			}
		}
		self.buffer.shrink_to_fit();
		match err {
			Some(e) => Err(e),
			None => Ok(()),
		}
	}
}

struct ProgressGenerator {
	sink: BufferedSender,
}

impl DiagnosticsSink for ProgressGenerator {
	fn progress(&mut self, progress: Progress) {
		let ratio = match progress {
			Progress::Ratio(v) => v,
			Progress::Range { cur, max } => (cur as f64) / (max as f64),
			Progress::Complete => 1.0,
			Progress::Count(_) => todo!(),
		};
		let _: StdResult<_, _> = self.sink.send(RpcMessage::ProgressPush(ratio));
	}

	fn log(&mut self, level: diag::Level, subsystem: &str, message: &str) {
		let _: StdResult<_, _> = self.sink.send(RpcMessage::DiagnosticsLog(
			level,
			subsystem.to_string(),
			message.to_string(),
		));
	}
}

pub struct RpcStoreServerWorker<S> {
	inner: Arc<S>,
	rx_ch: mpsc::Receiver<RpcWorkerMessage>,
	// required to prevent the rpc worker from shutting down
	#[allow(dead_code)]
	guard: mpsc::Sender<RpcWorkerCommand>,
}

impl<S: ObjectStore + Sync + Send + 'static> RpcStoreServerWorker<S> {
	async fn stream_objects(backend: Arc<S>, ids: Vec<Id>, ctx: MessageSender) -> Result<()> {
		let mut stream = backend.stream_objects(ids)?;
		while let Some(item) = stream.next().await {
			let item = item?;
			match ctx.send(RpcMessage::StreamedChunk(Ok(item))).await {
				Ok(_) => (),
				Err(MessageSendError::Other(e)) => Err(e)?,
				Err(MessageSendError::AlreadyComplete) => unreachable!(),
			}
		}
		Ok(())
	}

	async fn run(mut self) {
		loop {
			tokio::select! {
				msg = self.rx_ch.recv() => match msg {
					Some((ctx, payload)) => {
						let backend = Arc::clone(&self.inner);
						tokio::spawn(async move {
							let response = match payload {
								RpcRequest::RetrieveObject{id} => {
									backend.retrieve(id).await.into()
								}
								RpcRequest::ContainsObject{id} => {
									backend.contains(id).await.into()
								}
								RpcRequest::GetRepositoryConfigKey{key} => {
									backend.get_repository_config_key(&key).await.into()
								}
								RpcRequest::CheckRepository => {
									let mut progress_sink = ProgressGenerator{sink: BufferedSender::wrap(ctx.message_sender())};
									let result = backend.check_all_segments(Some(&mut progress_sink)).await.into();
									let _: StdResult<_, _> = progress_sink.sink.flush().await;
									result
								}
								RpcRequest::StreamObjects{ids} => {
									Self::stream_objects(backend, ids, ctx.message_sender()).await.into()
								}
								RpcRequest::FindMissingObjects{ids} => {
									backend.find_missing_objects(ids).await.into()
								}
							};
							let _: StdResult<_, _> = ctx.reply(response).await;
						});
					}
					None => return,
				}
			}
		}
	}
}

pub fn spawn_rpc_server<
	S: ObjectStore + Send + Sync + 'static,
	I: AsyncRead + AsyncWrite + Send + 'static,
>(
	backend: S,
	io: I,
) -> tokio::task::JoinHandle<()> {
	let (_, command_tx, message_rx) = RpcWorkerConfig::with_name_prefix("server").spawn(io);
	let worker = RpcStoreServerWorker {
		inner: Arc::new(backend),
		rx_ch: message_rx,
		guard: command_tx,
	};
	tokio::spawn(worker.run())
}
