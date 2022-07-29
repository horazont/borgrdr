//! Backend for accessing a borg repository
use std::io;
use std::io::BufRead;

use bytes::{Buf, Bytes};

use super::segments::Id;

pub trait ObjectStore {
	fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes>;

	fn open_stream<A: AsRef<Id>, I: Iterator<Item = A>>(
		&self,
		iter: I,
	) -> StreamReader<'_, Self, I> {
		StreamReader {
			inner: self,
			curr: None,
			iter,
		}
	}
}

pub struct StreamReader<'x, S: ?Sized, I> {
	inner: &'x S,
	curr: Option<Bytes>,
	iter: I,
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> io::Read for StreamReader<'x, S, I> {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let src = self.fill_buf()?;
		let to_copy = src.len().min(buf.len());
		buf[..to_copy].copy_from_slice(&src[..to_copy]);
		self.consume(to_copy);
		Ok(to_copy)
	}
}

impl<'x, A: AsRef<Id>, S: ObjectStore, I: Iterator<Item = A>> io::BufRead
	for StreamReader<'x, S, I>
{
	fn consume(&mut self, amt: usize) {
		if amt == 0 {
			return;
		}
		// unwrap: if amt > 0, the caller must've called fill_buf and received
		// a non-empty buffer; otherwise, it's ok to panic.
		let curr = self.curr.as_mut().unwrap();
		curr.advance(amt);
		if curr.remaining() == 0 {
			self.curr = None;
		}
	}

	fn fill_buf(&mut self) -> io::Result<&[u8]> {
		loop {
			// skip empty buffers
			match self.curr.as_ref().map(|x| x.remaining() == 0) {
				// empty
				Some(true) | None => {
					let next = self
						.iter
						.next()
						.map(|x| self.inner.retrieve(x))
						.transpose()?;
					if next.is_none() {
						// eof
						self.curr = None;
						return Ok(&[]);
					}
				}
				Some(false) => break,
			}
		}
		Ok(self.curr.as_ref().unwrap().chunk())
	}
}
