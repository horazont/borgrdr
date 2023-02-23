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
use crate::pipeline::ObjectStream;
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

enum RpcState {
	NotStarted {
		req: RpcRequest,
		message_sink: Option<mpsc::Sender<RpcMessage>>,
		request_ch: tokio_util::sync::PollSender<RpcWorkerCommand>,
	},
	Pending {
		response_rx: oneshot::Receiver<io::Result<RpcResponse>>,
	},
	Completed,
}

struct Rpc {
	state: RpcState,
}

impl Rpc {
	fn start(
		request_ch: mpsc::Sender<RpcWorkerCommand>,
		req: RpcRequest,
		message_sink: Option<mpsc::Sender<RpcMessage>>,
	) -> Self {
		Self {
			state: RpcState::NotStarted {
				request_ch: tokio_util::sync::PollSender::new(request_ch),
				req,
				message_sink,
			},
		}
	}
}

impl Future for Rpc {
	type Output = Result<RpcResponse>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		if let RpcState::NotStarted { request_ch, .. } = &mut self.state {
			match request_ch.poll_reserve(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(Err(_)) => {
					self.state = RpcState::Completed;
					return Poll::Ready(Err(Error::LostWorker));
				}
				Poll::Ready(Ok(())) => (),
			}
			let mut state = RpcState::Completed;
			std::mem::swap(&mut state, &mut self.state);
			let (req, message_sink, mut request_ch) = match state {
				RpcState::NotStarted {
					req,
					message_sink,
					request_ch,
				} => (req, message_sink, request_ch),
				_ => unreachable!(),
			};
			let (response_tx, response_rx) = oneshot::channel();
			match request_ch.send_item((req, message_sink, response_tx)) {
				Ok(()) => (),
				Err(_) => return Poll::Ready(Err(Error::LostWorker)),
			};
			self.state = RpcState::Pending { response_rx };
		}

		if let RpcState::Pending {
			ref mut response_rx,
		} = &mut self.state
		{
			return match Pin::new(response_rx).poll(cx) {
				Poll::Pending => Poll::Pending,
				Poll::Ready(v) => {
					self.state = RpcState::Completed;
					Poll::Ready(match v {
						Ok(Ok(RpcResponse::Error(remote_err))) => Err(Error::Remote(remote_err)),
						Ok(Ok(other)) => Ok(other),
						Ok(Err(e)) => Err(Error::Communication(e)),
						Err(_) => Err(Error::LostWorker),
					})
				}
			};
		}

		panic!("invalid future state")
	}
}

pin_project_lite::pin_project! {
	pub struct DataRpc {
		#[pin]
		inner: Rpc,
	}
}

impl Future for DataRpc {
	type Output = io::Result<Bytes>;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();
		match this.inner.poll(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(v) => Poll::Ready(
				match_rpc_response! {
					v => {
						Ok(RpcResponse::DataReply(data)) => Ok(data),
					}
				}
				.map_err(|x| x.into()),
			),
		}
	}
}

#[async_trait::async_trait]
impl ObjectStore for RpcStoreClient {
	type RetrieveFut<'x> = DataRpc;

	fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> Self::RetrieveFut<'_> {
		let id = *id.as_ref();
		DataRpc {
			inner: Rpc::start(
				self.request_ch.clone(),
				RpcRequest::RetrieveObject { id },
				None,
			),
		}
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
		let mut stream = ObjectStream::open(&backend, ids.into_iter());
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
