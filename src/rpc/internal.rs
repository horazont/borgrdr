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
	stream_block_size: usize,
}

impl RpcStoreClient {
	pub fn new<I: AsyncRead + AsyncWrite + Send + 'static>(inner: I) -> Self {
		let (_, ch_tx, _) = RpcWorkerConfig::with_name_prefix("client").spawn(inner);
		Self {
			request_ch: ch_tx,
			stream_block_size: 128,
		}
	}

	pub async fn new_borg<I: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
		inner: I,
		repository_path: String,
	) -> io::Result<Self> {
		let (_, ch_tx, _) = RpcWorkerConfig::with_name_prefix("client")
			.spawn_borg(inner, repository_path)
			.await?;
		Ok(Self {
			request_ch: ch_tx,
			stream_block_size: 1,
		})
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
		Ok(ObjectStream {
			backend: tokio_util::sync::PollSender::new(self.request_ch.clone()),
			src: Chunker::new(object_ids, self.stream_block_size).peekable(),
			curr_chunk: ObjectStreamChunk::None,
		})
	}
}

enum Chunk {
	Single(Id),
	Block(Vec<Id>),
}

struct Chunker {
	inner: std::vec::IntoIter<Id>,
	block_size: usize,
}

impl Chunker {
	fn new(src: Vec<Id>, block_size: usize) -> Self {
		Self {
			inner: src.into_iter(),
			block_size,
		}
	}
}

impl Iterator for Chunker {
	type Item = Chunk;

	fn next(&mut self) -> Option<Self::Item> {
		if self.block_size == 1 {
			Some(Chunk::Single(self.inner.next()?))
		} else {
			let nitems = self
				.inner
				.size_hint()
				.1
				.unwrap_or(self.block_size)
				.min(self.block_size);
			let mut buf = Vec::with_capacity(nitems);
			for _ in 0..nitems {
				match self.inner.next() {
					Some(v) => buf.push(v),
					None => break,
				}
			}
			if buf.len() == 0 {
				None
			} else {
				Some(Chunk::Block(buf))
			}
		}
	}
}

pin_project_lite::pin_project! {
	#[project = ObjectStreamChunkProj]
	enum ObjectStreamChunk {
		None,
		Streamed{
			#[pin]
			inner: CompletionStream<RpcMessage, io::Result<RpcResponse>>,
		},
		Single{
			#[pin]
			inner: Option<oneshot::Receiver<io::Result<RpcResponse>>>,
		},
	}
}

impl ObjectStreamChunk {
	fn is_none(self: Pin<&mut Self>) -> bool {
		match self.project() {
			ObjectStreamChunkProj::None => true,
			_ => false,
		}
	}
}

impl Stream for ObjectStreamChunk {
	type Item = CompletionStreamItem<RpcMessage, io::Result<RpcResponse>>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.project();
		match this {
			ObjectStreamChunkProj::None => Poll::Ready(None),
			ObjectStreamChunkProj::Single { mut inner } => match inner.as_mut().as_pin_mut() {
				Some(v) => match v.poll(cx) {
					Poll::Ready(Ok(Ok(RpcResponse::DataReply(v)))) => {
						*inner = None;
						Poll::Ready(Some(CompletionStreamItem::Data(RpcMessage::StreamedChunk(
							Ok(v),
						))))
					}
					Poll::Ready(Ok(Ok(other))) => {
						*inner = None;
						Poll::Ready(Some(CompletionStreamItem::Completed(Err(
							Error::UnexpectedResponse(other).into(),
						))))
					}
					Poll::Ready(Ok(Err(e))) => {
						*inner = None;
						Poll::Ready(Some(CompletionStreamItem::Completed(Err(e))))
					}
					Poll::Ready(Err(_)) => {
						*inner = None;
						Poll::Ready(Some(CompletionStreamItem::Crashed))
					}
					Poll::Pending => Poll::Pending,
				},
				None => Poll::Ready(Some(CompletionStreamItem::Completed(Ok(
					RpcResponse::Success,
				)))),
			},
			ObjectStreamChunkProj::Streamed { inner } => inner.poll_next(cx),
		}
	}
}

pin_project_lite::pin_project! {
	pub struct ObjectStream {
		#[pin]
		backend: tokio_util::sync::PollSender<RpcWorkerCommand>,
		src: std::iter::Peekable<Chunker>,
		#[pin]
		curr_chunk: ObjectStreamChunk,
	}
}

impl Stream for ObjectStream {
	type Item = io::Result<Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		//println!("ObjectStream asked for more");
		let mut this = self.project();
		loop {
			if this.curr_chunk.as_mut().is_none() {
				//println!("ObjectStream needs to request more");
				if this.src.peek().is_none() {
					//println!("ObjectStream exhausted");
					return Poll::Ready(None);
				}

				match this.backend.as_mut().poll_reserve(cx) {
					Poll::Pending => return Poll::Pending,
					Poll::Ready(Err(_)) => return Poll::Ready(Some(Err(Error::LostWorker.into()))),
					Poll::Ready(Ok(())) => (),
				}

				// we can now prepare the sending :-)
				let chunk = this.src.next().unwrap();
				*this.curr_chunk = match chunk {
					Chunk::Single(id) => {
						let (result_tx, result_rx) = oneshot::channel();
						match this.backend.send_item((
							RpcRequest::RetrieveObject { id },
							None,
							result_tx,
						)) {
							Ok(()) => (),
							Err(_) => return Poll::Ready(Some(Err(Error::LostWorker.into()))),
						};
						ObjectStreamChunk::Single {
							inner: Some(result_rx),
						}
					}
					Chunk::Block(ids) => {
						let (result_tx, result_rx) = oneshot::channel();
						let (msg_tx, msg_rx) = mpsc::channel(ids.len());
						match this.backend.send_item((
							RpcRequest::StreamObjects { ids },
							Some(msg_tx),
							result_tx,
						)) {
							Ok(()) => (),
							Err(_) => return Poll::Ready(Some(Err(Error::LostWorker.into()))),
						};
						ObjectStreamChunk::Streamed {
							inner: CompletionStream::wrap(msg_rx, result_rx),
						}
					}
				};
			}

			//println!("ObjectStream asking inner stream");
			match this.curr_chunk.as_mut().poll_next(cx) {
				// forward streamed data to user
				Poll::Ready(Some(CompletionStreamItem::Data(RpcMessage::StreamedChunk(data)))) => {
					return Poll::Ready(Some(data.map_err(Error::Remote).map_err(|x| x.into())))
				}
				// ignore unexpected messages
				Poll::Ready(Some(CompletionStreamItem::Data(_))) => (),
				// if completed ...
				Poll::Ready(Some(CompletionStreamItem::Completed(Ok(v)))) => {
					*this.curr_chunk = ObjectStreamChunk::None;
					match v.result() {
						// ... with success, we try the next one
						Ok(_) => (),
						// ... with error, we return the error
						Err(e) => return Poll::Ready(Some(Err(e.into()))),
					}
				}
				// if completed with error (e.g. send error), we return the error
				Poll::Ready(Some(CompletionStreamItem::Completed(Err(e)))) => {
					*this.curr_chunk = ObjectStreamChunk::None;
					return Poll::Ready(Some(Err(e)));
				}
				// if completed unexpectedly, continue and inject LostWorker error
				Poll::Ready(None) | Poll::Ready(Some(CompletionStreamItem::Crashed)) => {
					*this.curr_chunk = ObjectStreamChunk::None;
					return Poll::Ready(Some(Err(Error::LostWorker.into())));
				}
				Poll::Pending => return Poll::Pending,
			}

			*this.curr_chunk = ObjectStreamChunk::None;
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
