use std::io;
use std::marker::PhantomData;

use log::{debug, trace};

use bytes::{Buf, BufMut, BytesMut};

use serde::{de::DeserializeOwned, ser, ser::Serialize, ser::Serializer};

use tokio_util::codec::{Decoder, Encoder};

use rmp_serde::{decode, encode};

struct EvilCompound<'a, W: 'a, C: 'a> {
	inner: &'a mut EvilSerializer<W, C>,
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeTuple
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::Compound<'a, W, C> as ser::SerializeTuple>::Ok;
	type Error = <encode::Compound<'a, W, C> as ser::SerializeTuple>::Error;

	fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		value.serialize(&mut *self.inner)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeTupleStruct
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::Compound<'a, W, C> as ser::SerializeTupleStruct>::Ok;
	type Error = <encode::Compound<'a, W, C> as ser::SerializeTupleStruct>::Error;

	fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		value.serialize(&mut *self.inner)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeTupleVariant
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::Compound<'a, W, C> as ser::SerializeTupleVariant>::Ok;
	type Error = <encode::Compound<'a, W, C> as ser::SerializeTupleVariant>::Error;

	fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		value.serialize(&mut *self.inner)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeStruct
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::Compound<'a, W, C> as ser::SerializeStruct>::Ok;
	type Error = <encode::Compound<'a, W, C> as ser::SerializeStruct>::Error;

	fn serialize_field<T: ?Sized + Serialize>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error> {
		C::write_struct_field(&mut *self.inner, key, value)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeStructVariant
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::Compound<'a, W, C> as ser::SerializeStruct>::Ok;
	type Error = <encode::Compound<'a, W, C> as ser::SerializeStruct>::Error;

	fn serialize_field<T: ?Sized + Serialize>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error> {
		C::write_struct_field(&mut *self.inner, key, value)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeSeq
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeSeq>::Ok;
	type Error = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeSeq>::Error;

	fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		value.serialize(&mut *self.inner)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeMap
	for EvilCompound<'a, W, C>
{
	type Ok = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeMap>::Ok;
	type Error = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeMap>::Error;

	fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
		<Self as ser::SerializeSeq>::serialize_element(self, key)
	}

	fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		<Self as ser::SerializeSeq>::serialize_element(self, value)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

struct EvilMaybeUnknownLengthCompound<'a, W: 'a, C: 'a> {
	inner: &'a mut EvilSerializer<W, C>,
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeSeq
	for EvilMaybeUnknownLengthCompound<'a, W, C>
{
	type Ok = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeSeq>::Ok;
	type Error = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeSeq>::Error;

	fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		todo!();
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		todo!();
	}
}

impl<'a, W: io::Write + 'a, C: rmp_serde::config::SerializerConfig + 'a> ser::SerializeMap
	for EvilMaybeUnknownLengthCompound<'a, W, C>
{
	type Ok = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeMap>::Ok;
	type Error = <encode::MaybeUnknownLengthCompound<'a, W, C> as ser::SerializeMap>::Error;

	fn serialize_key<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		todo!();
	}

	fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
		todo!();
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		todo!();
	}
}

struct EvilSerializer<W, C = rmp_serde::config::DefaultConfig> {
	inner: encode::Serializer<W, C>,
}

impl<W: io::Write, C> rmp_serde::encode::UnderlyingWrite for EvilSerializer<W, C> {
	type Write = W;

	fn get_ref(&self) -> &Self::Write {
		self.inner.get_ref()
	}

	fn get_mut(&mut self) -> &mut Self::Write {
		self.inner.get_mut()
	}

	fn into_inner(self) -> Self::Write {
		self.inner.into_inner()
	}
}

impl<'a, W: io::Write, C: rmp_serde::config::SerializerConfig> EvilSerializer<W, C> {
	fn compound(
		&'a mut self,
	) -> Result<EvilCompound<'a, W, C>, <&'a mut encode::Serializer<W, C> as Serializer>::Error> {
		Ok(EvilCompound { inner: self })
	}
}

impl<'a, W: io::Write, C: rmp_serde::config::SerializerConfig> Serializer
	for &'a mut EvilSerializer<W, C>
{
	type Ok = <&'a mut encode::Serializer<W, C> as Serializer>::Ok;
	type Error = <&'a mut encode::Serializer<W, C> as Serializer>::Error;
	type SerializeSeq = EvilCompound<'a, W, C>;
	type SerializeTuple = EvilCompound<'a, W, C>;
	type SerializeTupleStruct = EvilCompound<'a, W, C>;
	type SerializeTupleVariant = EvilCompound<'a, W, C>;
	type SerializeMap = EvilCompound<'a, W, C>;
	type SerializeStruct = EvilCompound<'a, W, C>;
	type SerializeStructVariant = EvilCompound<'a, W, C>;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_bool(v)
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		let w = self.inner.get_mut();
		rmp::encode::write_str_len(w, v.len() as u32)?;
		w.write_all(v)
			.map_err(rmp::encode::ValueWriteError::InvalidDataWrite)?;
		Ok(())
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_char(v)
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_f32(v)
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_f64(v)
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_i8(v)
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_i16(v)
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_i32(v)
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_i64(v)
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_u8(v)
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_u16(v)
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_u32(v)
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_u64(v)
	}

	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		let len = len.unwrap();
		rmp::encode::write_map_len(self.inner.get_mut(), len as u32)?;
		self.compound()
	}

	fn serialize_newtype_struct<T: Serialize + ?Sized>(
		self,
		name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error> {
		value.serialize(self)
	}

	fn serialize_newtype_variant<T: Serialize + ?Sized>(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error> {
		rmp::encode::write_map_len(self.inner.get_mut(), 1)?;
		C::write_variant_ident(self, variant_index, variant)?;
		value.serialize(self)
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_none()
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		let len = len.unwrap();
		rmp::encode::write_array_len(self.inner.get_mut(), len as u32)?;
		self.compound()
	}

	fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
		value.serialize(self)
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_str(v)
	}

	fn serialize_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		C::write_struct_len(self, len)?;
		self.compound()
	}

	fn serialize_struct_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		rmp::encode::write_map_len(self.inner.get_mut(), 1)?;
		C::write_variant_ident(self, variant_index, variant)?;
		self.serialize_struct(name, len)
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		rmp::encode::write_array_len(self.inner.get_mut(), len as u32)?;
		self.compound()
	}

	fn serialize_tuple_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		rmp::encode::write_array_len(self.inner.get_mut(), len as u32)?;
		self.compound()
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		rmp::encode::write_map_len(self.inner.get_mut(), 1)?;
		C::write_variant_ident(self, variant_index, variant)?;
		self.serialize_tuple(len)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_unit()
	}

	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
		self.inner.serialize_unit_struct(name)
	}

	fn serialize_unit_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		self.inner
			.serialize_unit_variant(name, variant_index, variant)
	}
}

pub struct AsymMpCodec<TX, RX>(bool, PhantomData<TX>, PhantomData<RX>);

impl<TX, RX> AsymMpCodec<TX, RX> {
	pub(crate) fn new() -> Self {
		Self(false, PhantomData, PhantomData)
	}

	#[allow(dead_code)]
	pub(crate) fn with_debug() -> Self {
		Self(true, PhantomData, PhantomData)
	}
}

pub type MpCodec<T> = AsymMpCodec<T, T>;

impl<RX: DeserializeOwned, TX> Decoder for AsymMpCodec<TX, RX> {
	type Item = RX;
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

impl<TX: Serialize, RX> Encoder<TX> for AsymMpCodec<TX, RX> {
	type Error = io::Error;

	fn encode(&mut self, item: TX, dst: &mut BytesMut) -> Result<(), Self::Error> {
		let mut wr = dst.writer();
		let mut se = EvilSerializer {
			inner: rmp_serde::encode::Serializer::new(&mut wr).with_struct_map(),
		};
		item.serialize(&mut se)
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidInput, x))
	}
}
