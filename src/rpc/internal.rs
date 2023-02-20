use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

use bytes::Bytes;

use futures::stream::{Stream, StreamExt};

use tokio_util::sync::PollSender;

use crate::diag;
use crate::diag::{DiagnosticsSink, Progress};
use crate::segments::Id;
use crate::store::ObjectStore;

use super::worker::{
	CompletionStream, CompletionStreamItem, Error, MessageSendError, MessageSender, Result,
	RpcMessage, RpcRequest, RpcResponse, RpcWorkerCommand, RpcWorkerConfig, RpcWorkerMessage,
	TryMessageSendError,
};

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

#[derive(Clone)]
pub struct RpcStoreClient {
	request_ch: mpsc::Sender<RpcWorkerCommand>,
}

impl RpcStoreClient {
	pub fn new<I: AsyncRead + AsyncWrite + Send + 'static>(inner: I) -> Self {
		let (_, ch_tx, _) = RpcWorkerConfig::with_name_prefix("client").spawn(inner);
		Self { request_ch: ch_tx }
	}

	pub async fn new_borg<I: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
		inner: I,
		repository_path: String,
	) -> io::Result<Self> {
		let (_, ch_tx, _) = RpcWorkerConfig::with_name_prefix("client")
			.spawn_borg(inner, repository_path)
			.await?;
		Ok(Self { request_ch: ch_tx })
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
				Ok(RpcResponse::DataReply(data)) => Ok(Some(String::from_utf8(data.into()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?)),
				Ok(RpcResponse::Nil) => Ok(None),
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
		let backend = self.request_ch.clone();
		let mut src = object_ids.into_iter().peekable();
		let mut buffer = ObjectStreamBuffer::new(8);
		buffer.try_fill(&backend, &mut src)?;
		let backend = PollSender::new(backend);
		Ok(ObjectStream {
			backend,
			src,
			buffer,
		})
	}
}

struct ObjectStreamBuffer {
	inner: VecDeque<oneshot::Receiver<io::Result<RpcResponse>>>,
}

impl ObjectStreamBuffer {
	fn new(depth: usize) -> Self {
		Self {
			inner: VecDeque::with_capacity(depth),
		}
	}

	fn try_fill<I: Iterator<Item = Id>>(
		&mut self,
		backend: &mpsc::Sender<RpcWorkerCommand>,
		src: &mut std::iter::Peekable<I>,
	) -> Result<()> {
		while self.inner.len() < self.inner.capacity() {
			if src.peek().is_none() {
				return Ok(());
			}
			let permit = match backend.try_reserve() {
				Ok(permit) => permit,
				Err(mpsc::error::TrySendError::Closed(())) => return Err(Error::LostWorker),
				Err(mpsc::error::TrySendError::Full(())) => return Ok(()),
			};
			let id = src.next().unwrap(); // peeked before
			let (result_tx, result_rx) = oneshot::channel();
			permit.send((RpcRequest::RetrieveObject { id }, None, result_tx));
			self.inner.push_back(result_rx);
		}
		Ok(())
	}

	fn poll_fill<I: Iterator<Item = Id>>(
		&mut self,
		mut backend: Pin<&mut PollSender<RpcWorkerCommand>>,
		src: &mut std::iter::Peekable<I>,
		cx: &mut Context<'_>,
	) -> Poll<Result<()>> {
		while self.inner.len() < self.inner.capacity() {
			if src.peek().is_none() {
				return Poll::Ready(Ok(()));
			}
			match backend.as_mut().poll_reserve(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(_)) => return Poll::Ready(Err(Error::LostWorker)),
				Poll::Ready(Ok(())) => (),
			}
			let id = src.next().unwrap(); // peeked before
			let (result_tx, result_rx) = oneshot::channel();
			match backend.send_item((RpcRequest::RetrieveObject { id }, None, result_tx)) {
				Ok(()) => (),
				Err(_) => return Poll::Ready(Err(Error::LostWorker)),
			};
			self.inner.push_back(result_rx);
		}
		Poll::Pending
	}

	fn poll_front(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<RpcResponse>>> {
		let item = match self.inner.front_mut() {
			None => return Poll::Ready(None),
			Some(item) => item,
		};
		match Pin::new(item).poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(result) => {
				self.inner.pop_front();
				match result {
					Ok(v) => Poll::Ready(Some(v)),
					Err(_) => Poll::Ready(Some(Err(Error::LostWorker.into()))),
				}
			}
		}
	}
}

pin_project_lite::pin_project! {
	pub struct ObjectStream {
		#[pin]
		backend: tokio_util::sync::PollSender<RpcWorkerCommand>,
		src: std::iter::Peekable<std::vec::IntoIter<Id>>,
		buffer: ObjectStreamBuffer,
	}
}

impl Stream for ObjectStream {
	type Item = io::Result<Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		//println!("ObjectStream asked for more");
		let mut this = self.project();
		// we ignore the result of *this* polling, because we only do this
		// for opportunistic reasons.
		let _: Poll<_> = this.buffer.poll_fill(this.backend.as_mut(), this.src, cx);
		match this.buffer.poll_front(cx) {
			Poll::Ready(Some(v)) => match v {
				Ok(RpcResponse::DataReply(data)) => return Poll::Ready(Some(Ok(data))),
				Ok(other) => {
					return Poll::Ready(Some(Err(Error::UnexpectedResponse(other).into())))
				}
				Err(e) => return Poll::Ready(Some(Err(e))),
			},
			Poll::Pending => return Poll::Pending,
			// nothing in the buffer right now, try to fill some again below.
			Poll::Ready(None) => (),
		};

		match this.buffer.poll_fill(this.backend.as_mut(), this.src, cx) {
			// there is nothing left in the buffer, and nothing in src.
			Poll::Ready(Ok(())) => Poll::Ready(None),
			// there is nothing left in the buffer, and we cannot fill it because of errors.
			Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
			Poll::Pending => Poll::Pending,
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
