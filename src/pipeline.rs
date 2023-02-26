use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};

use bytes::Bytes;

use crate::segments::Id;
use crate::store::ObjectStore;

enum BufferItem<'x, M: 'x + Unpin> {
	Delimiter(M),
	Future(Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'x>>),
	Completed(io::Result<Bytes>),
	MovedOutFrom,
}

impl<'x, M: 'x + Unpin> BufferItem<'x, M> {
	fn is_ready(&self) -> bool {
		match self {
			Self::Delimiter(_) | Self::Completed(_) => true,
			Self::Future(_) => false,
			_ => panic!("buffer item already moved out from"),
		}
	}

	fn drive(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		match self.as_mut().get_mut() {
			Self::MovedOutFrom => panic!("buffer item already moved out from"),
			Self::Delimiter(_) | Self::Completed(_) => Poll::Ready(()),
			Self::Future(fut) => match fut.as_mut().poll(cx) {
				Poll::Pending => Poll::Pending,
				Poll::Ready(v) => {
					let mut new = BufferItem::Completed(v);
					std::mem::swap(&mut new, self.as_mut().get_mut());
					Poll::Ready(())
				}
			},
		}
	}
}

impl<'x, M: 'x + Unpin> Future for BufferItem<'x, M> {
	type Output = PipelineItem<M, Bytes>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		match self.as_mut().get_mut() {
			Self::MovedOutFrom => panic!("buffer item already moved out from"),
			Self::Delimiter(_) => {
				let mut buf = BufferItem::MovedOutFrom;
				std::mem::swap(&mut buf, self.as_mut().get_mut());
				match buf {
					Self::Delimiter(m) => Poll::Ready(PipelineItem::Delimiter(m)),
					_ => unreachable!(),
				}
			}
			Self::Completed(_) => {
				let mut buf = BufferItem::MovedOutFrom;
				std::mem::swap(&mut buf, self.as_mut().get_mut());
				match buf {
					Self::Completed(v) => Poll::Ready(PipelineItem::Data(v)),
					_ => unreachable!(),
				}
			}
			Self::Future(fut) => match fut.as_mut().poll(cx) {
				Poll::Pending => Poll::Pending,
				Poll::Ready(v) => {
					let mut new = BufferItem::MovedOutFrom;
					std::mem::swap(&mut new, self.as_mut().get_mut());
					Poll::Ready(PipelineItem::Data(v))
				}
			},
		}
	}
}

pin_project_lite::pin_project! {
	struct Buffer<'x, M: Unpin, II> {
		queue: VecDeque<BufferItem<'x, M>>,
		batch: Option<II>,
	}
}

impl<'x, M: 'x + Unpin, II> Default for Buffer<'x, M, II> {
	fn default() -> Self {
		Self::new(8)
	}
}

impl<'x, M: 'x + Unpin, II> Buffer<'x, M, II> {
	fn new(depth: usize) -> Self {
		// TODO: VecDeque may actually allocate more than requested, which means we need to cap the depth ourselves if we want to avoid going deeper than we planned here.
		Self {
			queue: VecDeque::with_capacity(depth),
			batch: None,
		}
	}
}

impl<'x, M: 'x + Unpin, IdT: AsRef<Id>, II: Iterator<Item = IdT>> Buffer<'x, M, II> {
	fn poll_fill(
		&mut self,
		store: &(impl ObjectStore + Send + Sync + Clone + 'static),
		mut src: impl Iterator<Item = (M, II)>,
	) -> Poll<()> {
		while self.queue.len() < self.queue.capacity() {
			log::trace!(
				"poll_fill: len={}/cap={}",
				self.queue.len(),
				self.queue.capacity()
			);
			match self.batch.as_mut().map(|x| x.next()).flatten() {
				Some(id) => {
					log::trace!("poll_fill: current batch has item");
					let store = store.clone();
					let id = *id.as_ref();
					let task = Box::pin(async move { store.retrieve(id).await });
					self.queue.push_back(BufferItem::Future(task));
					log::trace!(
						"poll_fill: len={}/cap={} (item from current batch)",
						self.queue.len(),
						self.queue.capacity()
					);
				}
				None => {
					log::trace!("poll_fill: current batch empty");
					// no more items in current batch, need to get next batch
					let (metadata, batch) = match src.next() {
						// no more batches
						None => {
							log::trace!("poll_fill: source depleted, exiting");
							return Poll::Ready(());
						}
						Some(v) => v,
					};
					self.batch = Some(batch);
					self.queue.push_back(BufferItem::Delimiter(metadata));
					log::trace!(
						"poll_fill: len={}/cap={} (delimiter)",
						self.queue.len(),
						self.queue.capacity()
					);
				}
			}
		}
		log::trace!(
			"poll_fill: len={}/cap={} (at capacity)",
			self.queue.len(),
			self.queue.capacity()
		);
		Poll::Pending
	}

	fn poll_front(&mut self, cx: &mut Context<'_>) -> Poll<PipelineItem<M, Bytes>> {
		// we need to drive all futures in the queue in order to allow them to advance
		for item in self.queue.iter_mut() {
			// we don't care about the poll result, as drive is idempotent and we gather the true result later.
			let _ = Pin::new(item).drive(cx);
		}
		if self.queue.front().map(|x| x.is_ready()).unwrap_or(false) {
			let mut item = self.queue.pop_front().unwrap();
			match Pin::new(&mut item).poll(cx) {
				Poll::Ready(v) => Poll::Ready(v),
				Poll::Pending => {
					// this absolutely should not happen...
					self.queue.push_front(item);
					Poll::Pending
				}
			}
		} else {
			Poll::Pending
		}
	}
}

pub enum PipelineItem<M, D> {
	Delimiter(M),
	Data(io::Result<D>),
}

impl<M, D> fmt::Debug for PipelineItem<M, D> {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Delimiter(_) => f.write_str("PipelineItem::Delimiter(..)"),
			Self::Data(Ok(_)) => f.write_str("PipelineItem::Data(Ok(..))"),
			Self::Data(Err(e)) => write!(f, "PipelineItem::Data(Err({:?}))", e),
		}
	}
}

pub struct Pipeline<'x, O, M: 'x + Unpin, II, IO> {
	store: &'x O,
	src: IO,
	buffer: Buffer<'x, M, II>,
}

impl<
		'x,
		O: ObjectStore + Send + Sync + Clone + 'static,
		M: Unpin,
		IdT: AsRef<Id>,
		II: Iterator<Item = IdT>,
		IO: Iterator<Item = (M, II)> + Unpin,
	> Pipeline<'x, O, M, II, IO>
{
	pub(crate) fn start(store: &'x O, src: IO) -> Self {
		Self {
			store,
			src,
			buffer: Buffer::default(),
		}
	}
}

impl<
		'x,
		O: ObjectStore + Send + Sync + Clone + 'static,
		M: Unpin,
		IdT: AsRef<Id>,
		II: Iterator<Item = IdT>,
		IO: Iterator<Item = (M, II)> + Unpin,
	> Stream for Pipeline<'x, O, M, II, IO>
{
	type Item = PipelineItem<M, Bytes>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.as_mut().get_mut();
		let _: Poll<_> = this.buffer.poll_fill(this.store, &mut this.src);
		let r = match this.buffer.poll_front(cx) {
			Poll::Ready(v) => {
				log::trace!("Pipeline: item ready: {:?}", v);
				Some(Poll::Ready(Some(v)))
			}
			Poll::Pending => {
				if this.buffer.queue.len() > 0 {
					Some(Poll::Pending)
				} else {
					// if there is nothing in the buffer
					None
				}
			}
		};
		match this.buffer.poll_fill(this.store, &mut this.src) {
			Poll::Pending => r.unwrap_or(Poll::Pending),
			// = end of stream, but only return that if we don't have an authoritative result from above'
			Poll::Ready(()) => r.unwrap_or(Poll::Ready(None)),
		}
	}
}

enum StreamState<M> {
	Running,
	Break { next_identity: M },
	Completed,
}

pin_project_lite::pin_project! {
	pub struct PipelineStream<M, I> {
		#[pin]
		inner: I,
		state: StreamState<M>,
	}
}

pub trait SegmentedStream {
	type Delimiter: Unpin;

	fn next_segment(&mut self) -> Option<Self::Delimiter>;
}

impl<M: Unpin, I: Stream<Item = PipelineItem<M, Bytes>> + Unpin> PipelineStream<M, I> {
	pub(crate) async fn new(mut inner: I) -> io::Result<Self> {
		let state = match inner.next().await {
			Some(PipelineItem::Delimiter(m)) => StreamState::Break { next_identity: m },
			Some(PipelineItem::Data(Err(e))) => return Err(e),
			Some(PipelineItem::Data(Ok(_))) => panic!("stream is not at beginning"),
			None => StreamState::Completed,
		};
		Ok(Self { inner, state })
	}
}

impl<M: Unpin, I: Stream<Item = PipelineItem<M, Bytes>>> SegmentedStream for PipelineStream<M, I> {
	type Delimiter = M;

	fn next_segment(&mut self) -> Option<Self::Delimiter> {
		match &mut self.state {
			StreamState::Running => panic!("substream not completed yet"),
			StreamState::Break { .. } => {
				let mut state = StreamState::Running;
				std::mem::swap(&mut state, &mut self.state);
				match state {
					StreamState::Break { next_identity } => Some(next_identity),
					_ => unreachable!(),
				}
			}
			StreamState::Completed => None,
		}
	}
}

impl<M: Unpin, I: Stream<Item = PipelineItem<M, Bytes>>> Stream for PipelineStream<M, I> {
	type Item = io::Result<Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();

		match this.state {
			StreamState::Completed | StreamState::Break { .. } => return Poll::Ready(None),
			StreamState::Running => (),
		};

		match this.inner.as_mut().poll_next(cx) {
			Poll::Pending => Poll::Pending,
			Poll::Ready(None) => {
				*this.state = StreamState::Completed;
				Poll::Ready(None)
			}
			Poll::Ready(Some(PipelineItem::Delimiter(next_identity))) => {
				*this.state = StreamState::Break { next_identity };
				Poll::Ready(None)
			}
			Poll::Ready(Some(PipelineItem::Data(data))) => Poll::Ready(Some(data)),
		}
	}
}

pin_project_lite::pin_project! {
	pub struct ObjectStream<'x, S, I> {
		#[pin]
		inner: PipelineStream<(), Pipeline<'x, S, (), I, std::iter::Once<((), I)>>>,
		ready: bool,
	}
}

impl<'x, S: ObjectStore + Send + Sync + Clone + 'static, I: Iterator<Item = Id> + Unpin>
	ObjectStream<'x, S, I>
{
	pub fn open(store: &'x S, ids: I) -> Self {
		log::trace!("ObjectStream opened");
		Self {
			inner: PipelineStream {
				inner: Pipeline {
					store,
					src: std::iter::once(((), ids)),
					buffer: Buffer::default(),
				},
				// the first poll will take care of that as we are ready = false
				state: StreamState::Completed,
			},
			ready: false,
		}
	}
}

impl<'x, S: ObjectStore + Send + Sync + Clone + 'static, I: Iterator<Item = Id> + Unpin> Stream
	for ObjectStream<'x, S, I>
{
	type Item = io::Result<Bytes>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let mut this = self.project();
		if !*this.ready {
			log::trace!("ObjectStream not ready yet, polling first item");
			let inner_proj = this.inner.as_mut().project();
			match inner_proj.inner.poll_next(cx) {
				Poll::Pending => return Poll::Pending,
				Poll::Ready(None) => {
					*inner_proj.state = StreamState::Completed;
					*this.ready = true;
					log::trace!("ObjectStream at EOF before readiness");
					return Poll::Ready(None);
				}
				Poll::Ready(Some(PipelineItem::Delimiter(()))) => {
					*inner_proj.state = StreamState::Running;
					*this.ready = true;
					log::trace!("ObjectStream received delimiter, marking as ready");
				}
				Poll::Ready(Some(PipelineItem::Data(v))) => {
					*inner_proj.state = StreamState::Running;
					log::trace!("ObjectStream received data as first thing, a bit odd, but returning and marking as ready");
					*this.ready = true;
					return Poll::Ready(Some(v));
				}
			}
		}

		let r = this.inner.poll_next(cx);
		log::trace!("ObjectStream polled: {:?}", r);
		r
	}
}
