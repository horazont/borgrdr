use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::ops::ControlFlow;
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
use crate::store::{ObjectStore, PrefetchStreamItem};

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
		let mut backend = PollSender::new(backend);
		buffer.try_fill(
			RpcRetrieveStarter {
				backend: &mut backend,
			},
			&mut src,
		)?;
		Ok(ObjectStream {
			backend,
			src,
			buffer,
		})
	}

	type PrefetchStream<
		M: 'static + Copy + Sync + Send + Unpin,
		II: 'static + Send + Sync + Iterator<Item = Id>,
		IO: 'static + Send + Sync + Iterator<Item = (M, II)>,
	> = PrefetchStream<M, II, IO>;

	fn stream_objects_with_prefetch<
		M: 'static + Copy + Sync + Send + Unpin,
		II: 'static + Iterator<Item = Id> + Send + Sync,
		IO: 'static + Iterator<Item = (M, II)> + Send + Sync,
	>(
		&self,
		groups: IO,
	) -> io::Result<Self::PrefetchStream<M, II, IO>> {
		let backend = self.request_ch.clone();
		let mut src = groups;
		let mut buffer = PrefetchStreamBuffer::new(8);
		let mut backend = PollSender::new(backend);
		buffer.try_fill(
			RpcRetrieveStarter {
				backend: &mut backend,
			},
			&mut src,
		)?;
		Ok(PrefetchStream {
			backend,
			buffer,
			src,
		})
	}
}

struct RpcRetrieveStarter<'x> {
	backend: &'x mut tokio_util::sync::PollSender<RpcWorkerCommand>,
}

impl<'x> RpcRetrieveStarter<'x> {
	fn poll_start<I: Iterator<Item = Id>>(
		&mut self,
		id_iter: &mut std::iter::Peekable<I>,
		cx: &mut Context<'_>,
	) -> Poll<io::Result<ReceiverWrapper<RpcResponse>>> {
		match self.backend.poll_reserve(cx) {
			Poll::Pending => return Poll::Pending,
			Poll::Ready(Err(_)) => return Poll::Ready(Err(Error::LostWorker.into())),
			Poll::Ready(Ok(())) => (),
		}
		let id = id_iter.next().unwrap(); // peeked before
		let (result_tx, result_rx) = oneshot::channel();
		match self
			.backend
			.send_item((RpcRequest::RetrieveObject { id }, None, result_tx))
		{
			Ok(()) => Poll::Ready(Ok(result_rx.into())),
			Err(_) => Poll::Ready(Err(Error::LostWorker.into())),
		}
	}

	fn try_start<I: Iterator<Item = Id>>(
		&self,
		id_iter: &mut std::iter::Peekable<I>,
	) -> Option<io::Result<ReceiverWrapper<RpcResponse>>> {
		let backend = match self.backend.get_ref() {
			Some(v) => v,
			None => return Some(Err(Error::LostWorker.into())),
		};
		let permit = match backend.try_reserve() {
			Ok(permit) => permit,
			Err(mpsc::error::TrySendError::Closed(())) => {
				return Some(Err(Error::LostWorker.into()))
			}
			Err(mpsc::error::TrySendError::Full(())) => return None,
		};
		let id = id_iter.next().unwrap(); // peeked before
		let (result_tx, result_rx) = oneshot::channel();
		permit.send((RpcRequest::RetrieveObject { id }, None, result_tx));
		Some(Ok(result_rx.into()))
	}
}

struct ObjectStreamBuffer {
	inner: VecDeque<ReceiverWrapper<RpcResponse>>,
}

impl ObjectStreamBuffer {
	fn new(depth: usize) -> Self {
		Self {
			inner: VecDeque::with_capacity(depth),
		}
	}

	fn try_fill<I: Iterator<Item = Id>>(
		&mut self,
		starter: RpcRetrieveStarter<'_>,
		src: &mut std::iter::Peekable<I>,
	) -> Result<()> {
		while self.inner.len() < self.inner.capacity() {
			if src.peek().is_none() {
				return Ok(());
			}
			match starter.try_start(src) {
				None => break,
				Some(fut) => self.inner.push_back(fut?),
			}
		}
		Ok(())
	}

	fn poll_fill<I: Iterator<Item = Id>>(
		&mut self,
		mut starter: RpcRetrieveStarter<'_>,
		src: &mut std::iter::Peekable<I>,
		cx: &mut Context<'_>,
	) -> Poll<Result<()>> {
		while self.inner.len() < self.inner.capacity() {
			if src.peek().is_none() {
				return Poll::Ready(Ok(()));
			}
			match starter.poll_start(src, cx) {
				Poll::Pending => break,
				Poll::Ready(fut) => self.inner.push_back(fut?),
			}
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
				Poll::Ready(Some(result))
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
		let _: Poll<_> = this.buffer.poll_fill(
			RpcRetrieveStarter {
				backend: this.backend.as_mut().get_mut(),
			},
			this.src,
			cx,
		);
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

		match this.buffer.poll_fill(
			RpcRetrieveStarter {
				backend: this.backend.as_mut().get_mut(),
			},
			this.src,
			cx,
		) {
			// there is nothing left in the buffer, and nothing in src.
			Poll::Ready(Ok(())) => Poll::Ready(None),
			// there is nothing left in the buffer, and we cannot fill it because of errors.
			Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
			Poll::Pending => Poll::Pending,
		}
	}
}

pub struct PrefetchFlattener<M, II, IO> {
	outer: IO,
	current: Option<(M, II)>,
}

impl<M: Copy, T, II: Iterator<Item = T>, IO: Iterator<Item = (M, II)>> Iterator
	for PrefetchFlattener<M, II, IO>
{
	type Item = (M, T);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			match self.current.as_mut() {
				Some((m, inner)) => match inner.next() {
					Some(item) => return Some((*m, item)),
					None => (),
				},
				None => (),
			}
			self.current = None;
			match self.outer.next() {
				Some(v) => {
					self.current = Some(v);
				}
				None => return None,
			};
		}
	}
}

struct ReceiverWrapper<T>(oneshot::Receiver<io::Result<T>>);

impl<T> From<oneshot::Receiver<io::Result<T>>> for ReceiverWrapper<T> {
	fn from(other: oneshot::Receiver<io::Result<T>>) -> Self {
		Self(other)
	}
}

impl<T> Future for ReceiverWrapper<T> {
	type Output = io::Result<T>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match Pin::new(&mut self.as_mut().get_mut().0).poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(Ok(v)) => Poll::Ready(v),
			Poll::Ready(Err(_)) => Poll::Ready(Err(Error::LostWorker.into())),
		}
	}
}

enum PrefetchQueueItem<M, F> {
	Header(M),
	Future(F),
	Completed,
}

impl<T, M: Unpin, F: Unpin + Future<Output = io::Result<T>>> Future for PrefetchQueueItem<M, F> {
	type Output = PrefetchStreamItem<M, T>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.as_mut().get_mut() {
			Self::Completed => panic!("prefetch queue item already completed"),
			Self::Header(_) => {
				let mut data = Self::Completed;
				std::mem::swap(&mut data, self.get_mut());
				match data {
					Self::Header(m) => Poll::Ready(PrefetchStreamItem::Metadata(m)),
					_ => unreachable!(),
				}
			}
			Self::Future(ref mut f) => match Pin::new(f).poll(cx) {
				Poll::Pending => Poll::Pending,
				Poll::Ready(v) => {
					let mut data = Self::Completed;
					std::mem::swap(&mut data, self.get_mut());
					Poll::Ready(PrefetchStreamItem::Data(v))
				}
			},
		}
	}
}

struct PrefetchStreamBuffer<M, II: Iterator, F> {
	queue: VecDeque<PrefetchQueueItem<M, F>>,
	batch: Option<std::iter::Peekable<II>>,
}

impl<M: Unpin, II: Iterator<Item = Id>> PrefetchStreamBuffer<M, II, ReceiverWrapper<RpcResponse>> {
	fn new(depth: usize) -> Self {
		Self {
			queue: VecDeque::with_capacity(depth),
			batch: None,
		}
	}

	#[must_use]
	fn ensure_more<IO: Iterator<Item = (M, II)>>(&mut self, src: &mut IO) -> ControlFlow<(), bool> {
		if self.batch.as_mut().map(|x| x.peek()).flatten().is_none() {
			let (metadata, new_batch) = match src.next() {
				Some(v) => v,
				None => return ControlFlow::Break(()),
			};
			self.queue.push_back(PrefetchQueueItem::Header(metadata));
			self.batch = Some(new_batch.peekable());
			// ensure the loop condition is checked here once more
			ControlFlow::Continue(true)
		} else {
			ControlFlow::Continue(false)
		}
	}

	fn try_fill<IO: Iterator<Item = (M, II)>>(
		&mut self,
		starter: RpcRetrieveStarter<'_>,
		mut src: IO,
	) -> Result<()> {
		while self.queue.len() < self.queue.capacity() {
			match self.ensure_more(&mut src) {
				// eof!
				ControlFlow::Break(()) => break,
				// inserted something -> need to re-check loop condition
				ControlFlow::Continue(true) => continue,
				// more data available without insertion
				ControlFlow::Continue(false) => (),
			}
			match starter.try_start(self.batch.as_mut().unwrap()) {
				Some(fut) => self.queue.push_back(PrefetchQueueItem::Future(fut?)),
				// no slot right now, stop here
				None => return Ok(()),
			}
		}
		Ok(())
	}

	fn poll_fill<IO: Iterator<Item = (M, II)>>(
		&mut self,
		mut starter: RpcRetrieveStarter<'_>,
		mut src: IO,
		cx: &mut Context<'_>,
	) -> Poll<Result<()>> {
		while self.queue.len() < self.queue.capacity() {
			match self.ensure_more(&mut src) {
				// eof!
				ControlFlow::Break(()) => return Poll::Ready(Ok(())),
				// inserted something -> need to re-check loop condition
				ControlFlow::Continue(true) => continue,
				// more data available without insertion
				ControlFlow::Continue(false) => (),
			}
			match starter.poll_start(self.batch.as_mut().unwrap(), cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(fut) => self.queue.push_back(PrefetchQueueItem::Future(fut?)),
			}
		}
		Poll::Pending
	}

	fn poll_front(
		&mut self,
		cx: &mut Context<'_>,
	) -> Poll<Option<PrefetchStreamItem<M, RpcResponse>>> {
		let item = match self.queue.front_mut() {
			None => return Poll::Ready(None),
			Some(item) => item,
		};
		match Pin::new(item).poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(result) => {
				self.queue.pop_front();
				Poll::Ready(Some(result))
			}
		}
	}
}

pin_project_lite::pin_project! {
	pub struct PrefetchStream<M, II: Iterator, IO: Iterator> {
		#[pin]
		backend: tokio_util::sync::PollSender<RpcWorkerCommand>,
		src: IO,
		buffer: PrefetchStreamBuffer<M, II, ReceiverWrapper<RpcResponse>>,
	}
}

impl<'x, M: 'static + Copy + Unpin, II: Iterator<Item = Id>, IO: Iterator<Item = (M, II)>> Stream
	for PrefetchStream<M, II, IO>
{
	type Item = PrefetchStreamItem<M, Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();
		// we ignore the result of *this* polling, because we only do this
		// for opportunistic reasons.
		let _: Poll<_> = this.buffer.poll_fill(
			RpcRetrieveStarter {
				backend: this.backend.as_mut().get_mut(),
			},
			&mut this.src,
			cx,
		);
		match this.buffer.poll_front(cx) {
			Poll::Ready(Some(v)) => match v {
				PrefetchStreamItem::Metadata(m) => {
					return Poll::Ready(Some(PrefetchStreamItem::Metadata(m)))
				}
				PrefetchStreamItem::Data(Ok(RpcResponse::DataReply(data))) => {
					return Poll::Ready(Some(PrefetchStreamItem::Data(Ok(data))))
				}
				PrefetchStreamItem::Data(Ok(other)) => {
					return Poll::Ready(Some(PrefetchStreamItem::Data(Err(
						Error::UnexpectedResponse(other).into(),
					))))
				}
				PrefetchStreamItem::Data(Err(e)) => {
					return Poll::Ready(Some(PrefetchStreamItem::Data(Err(e))))
				}
			},
			Poll::Pending => return Poll::Pending,
			// nothing in the buffer right now, try to fill some again below.
			Poll::Ready(None) => (),
		};

		match this.buffer.poll_fill(
			RpcRetrieveStarter {
				backend: this.backend.as_mut().get_mut(),
			},
			&mut this.src,
			cx,
		) {
			// there is nothing left in the buffer, and nothing in src.
			Poll::Ready(Ok(())) => Poll::Ready(None),
			// there is nothing left in the buffer, and we cannot fill it because of errors.
			Poll::Ready(Err(e)) => Poll::Ready(Some(PrefetchStreamItem::Data(Err(e.into())))),
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
