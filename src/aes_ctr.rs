//! There seems to be no sensible crate providing aes ctr. that's... sad.
use aes::cipher::{BlockEncrypt, KeyInit};

use byteorder::{BigEndian, ByteOrder, LittleEndian};

pub(crate) fn apply_ctr(key: &[u8], iv: &[u8], data: &mut [u8]) {
	let aes_block = aes::Aes256::new_from_slice(key).expect("key size mismatch");
	let mut input = [0u8; 16];
	input[..].copy_from_slice(iv);
	drop(iv);
	for chunk in data.chunks_mut(16) {
		let mut block = input.clone();
		aes_block.encrypt_block((&mut block).into());
		for i in 0..chunk.len() {
			let v = chunk[i];
			chunk[i] = v ^ block[i];
		}
		{
			let iv_numeric = BigEndian::read_u128(&input[..]);
			BigEndian::write_u128(&mut input[..], iv_numeric + 1);
		}
	}
}

pub(crate) fn apply_ctr_int(key: &[u8], iv: u128, data: &mut [u8]) {
	let mut iv_array = [0u8; 16];
	BigEndian::write_u128(&mut iv_array[..], iv);
	apply_ctr(key, &iv_array[..], data)
}
