use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};

use bytes::Bytes;

use crate::segments::Id;
use crate::store::ObjectStore;

enum BufferItem<M: Unpin> {
	Delimiter(M),
	Future(Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send + 'static>>),
	Completed(io::Result<Bytes>),
	MovedOutFrom,
}

impl<M: Unpin> BufferItem<M> {
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

impl<M: Unpin> Future for BufferItem<M> {
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
	struct Buffer<M: Unpin, II> {
		queue: VecDeque<BufferItem<M>>,
		batch: Option<II>,
	}
}

impl<M: Unpin, II: Iterator<Item = Id>> Buffer<M, II> {
	fn new(depth: usize) -> Self {
		Self {
			queue: VecDeque::with_capacity(depth),
			batch: None,
		}
	}

	fn poll_fill(
		&mut self,
		store: &(impl ObjectStore + Send + Sync + Clone + 'static),
		mut src: impl Iterator<Item = (M, II)>,
	) -> Poll<()> {
		while self.queue.len() < self.queue.capacity() {
			match self.batch.as_mut().map(|x| x.next()).flatten() {
				Some(id) => {
					let store = store.clone();
					let task = Box::pin(async move { store.retrieve(id).await });
					self.queue.push_back(BufferItem::Future(task));
				}
				None => {
					// no more items in current batch, need to get next batch
					let (metadata, batch) = match src.next() {
						// no more batches
						None => return Poll::Ready(()),
						Some(v) => v,
					};
					self.batch = Some(batch);
					self.queue.push_back(BufferItem::Delimiter(metadata));
				}
			}
		}
		Poll::Pending
	}

	fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<PipelineItem<M, Bytes>>> {
		// we need to drive all futures in the queue in order to allow them to advance
		for item in self.queue.iter_mut() {
			// we don't care about the poll result, as drive is idempotent and we gather the true result later.
			let _ = Pin::new(item).drive(cx);
		}
		if self.queue.front().map(|x| x.is_ready()).unwrap_or(false) {
			let mut item = self.queue.pop_front().unwrap();
			match Pin::new(&mut item).poll(cx) {
				Poll::Ready(v) => Poll::Ready(Some(v)),
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

pub struct Pipeline<'x, O, M: Unpin, II, IO> {
	store: &'x O,
	src: IO,
	buffer: Buffer<M, II>,
}

impl<
		'x,
		O: ObjectStore + Send + Sync + Clone + 'static,
		M: Unpin,
		II: Iterator<Item = Id>,
		IO: Iterator<Item = (M, II)> + Unpin,
	> Pipeline<'x, O, M, II, IO>
{
	pub(crate) fn start(store: &'x O, src: IO) -> Self {
		Self {
			store,
			src,
			buffer: Buffer::new(8),
		}
	}
}

impl<
		'x,
		O: ObjectStore + Send + Sync + Clone + 'static,
		M: Unpin,
		II: Iterator<Item = Id>,
		IO: Iterator<Item = (M, II)> + Unpin,
	> Stream for Pipeline<'x, O, M, II, IO>
{
	type Item = PipelineItem<M, Bytes>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.as_mut().get_mut();
		let _: Poll<_> = this.buffer.poll_fill(this.store, &mut this.src);
		match this.buffer.poll_next(cx) {
			Poll::Ready(v) => return Poll::Ready(v),
			Poll::Pending => (),
		}
		match this.buffer.poll_fill(this.store, &mut this.src) {
			Poll::Pending => Poll::Pending,
			// = end of stream
			Poll::Ready(()) => Poll::Ready(None),
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
