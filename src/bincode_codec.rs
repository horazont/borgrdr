use std::io;
use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use bincode::Options;

use tokio_util::codec::{length_delimited::LengthDelimitedCodec, Decoder, Encoder};

fn transpose_error(input: bincode::Error) -> io::Error {
	match *input {
		bincode::ErrorKind::Io(e) => e,
		other => io::Error::new(io::ErrorKind::InvalidData, other),
	}
}

pub(crate) struct BincodeCodec<T, O> {
	inner_codec: LengthDelimitedCodec,
	options: O,
	type_binding: PhantomData<fn() -> T>,
}

impl<T> BincodeCodec<T, bincode::DefaultOptions> {
	pub(crate) fn new() -> Self {
		Self {
			inner_codec: LengthDelimitedCodec::builder()
				.length_field_type::<u32>()
				.max_frame_length(256 * 1024 * 1024)
				.little_endian()
				.new_codec(),
			options: bincode::DefaultOptions::new(),
			type_binding: PhantomData,
		}
	}
}

impl<T: DeserializeOwned, O: Options + Clone> Decoder for BincodeCodec<T, O> {
	type Item = T;
	type Error = io::Error;

	fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
		let buf = match self.inner_codec.decode(src)? {
			None => return Ok(None),
			Some(buf) => buf,
		};
		match self.options.clone().deserialize(&buf) {
			Ok(v) => Ok(Some(v)),
			Err(e) => Err(transpose_error(e)),
		}
	}
}

impl<T: Serialize, O: Options + Clone> Encoder<T> for BincodeCodec<T, O> {
	type Error = io::Error;

	fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
		<Self as Encoder<&T>>::encode(self, &item, dst)
	}
}

impl<T: Serialize, O: Options + Clone> Encoder<&T> for BincodeCodec<T, O> {
	type Error = io::Error;

	fn encode(&mut self, item: &T, dst: &mut BytesMut) -> Result<(), Self::Error> {
		let mut buf = BytesMut::new();
		self.options
			.clone()
			.serialize_into((&mut buf).writer(), item)
			.map_err(transpose_error)?;
		self.inner_codec.encode(buf.freeze(), dst)
	}
}

pub(crate) type DefaultBincodeCodec<T> = BincodeCodec<T, bincode::DefaultOptions>;
