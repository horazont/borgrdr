use std::io;
use std::marker::PhantomData;

use log::{debug, trace};

use bytes::{Buf, BufMut, BytesMut};

use serde::{de::DeserializeOwned, ser::Serialize};

use tokio_util::codec::{Decoder, Encoder};

use rmp_serde::{decode, encode};

pub struct MpCodec<T>(bool, PhantomData<T>);

impl<T> MpCodec<T> {
	pub(crate) fn new() -> Self {
		Self(false, PhantomData)
	}

	#[allow(dead_code)]
	pub(crate) fn with_debug() -> Self {
		Self(true, PhantomData)
	}
}

impl<T: DeserializeOwned> Decoder for MpCodec<T> {
	type Item = T;
	type Error = io::Error;

	fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
		let mut buf = &src[..];
		let old_len = buf.len();
		trace!("asked to decode from {} bytes", old_len);
		match decode::from_read(&mut buf) {
			Ok(v) => {
				// consume and return
				let new_len = buf.len();
				let used = old_len - new_len;
				debug!(
					"decoded object from {} bytes ({} remaining in buffer)",
					used, new_len
				);
				src.advance(used);
				Ok(Some(v))
			}
			Err(decode::Error::InvalidDataRead(e)) | Err(decode::Error::InvalidMarkerRead(e))
				if e.kind() == io::ErrorKind::UnexpectedEof =>
			{
				trace!("eof while decoding next at remaining {} bytes", old_len);
				// not enough data in buffer, do not advance and try again
				Ok(None)
			}
			Err(other) => Err(io::Error::new(io::ErrorKind::InvalidData, other)),
		}
	}
}

impl<T: Serialize> Encoder<T> for MpCodec<T> {
	type Error = io::Error;

	fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
		encode::write(&mut dst.writer(), &item)
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidInput, x))
	}
}
