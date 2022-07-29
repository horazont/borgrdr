use ring::digest::{Context, SHA256};

use super::segments::Id;

#[repr(u8)]
pub enum KeyType {
	KeyfileKey = 0,
	PassphraseKey = 1,
	Unencrypted = 2,
	RepoKey = 3,
	Blake2KeyfileKey = 4,
	Blake2RepoKey = 5,
	Blake2AuthenticatedKey = 6,
	AuthenticatedKey = 7,
}

pub trait Key {
	fn decrypt(&self, src: &[u8]) -> Vec<u8>;
	fn encrypt(&self, src: &[u8]) -> Vec<u8>;
	fn key_type() -> KeyType;
}

pub trait ContentHasher {
	fn id(&self, content: &[u8]) -> Id;
}

pub struct Plaintext();

impl Plaintext {
	pub fn new() -> Self {
		Self()
	}
}

impl Key for Plaintext {
	fn key_type() -> KeyType {
		KeyType::Unencrypted
	}

	fn decrypt(&self, src: &[u8]) -> Vec<u8> {
		assert!(src[0] == 0x02);
		src[1..].to_vec()
	}

	fn encrypt(&self, src: &[u8]) -> Vec<u8> {
		let mut result = Vec::with_capacity(src.len() + 1);
		result.push(0x02);
		result.copy_from_slice(src);
		result
	}
}

impl ContentHasher for Plaintext {
	fn id(&self, content: &[u8]) -> Id {
		let mut ctx = Context::new(&SHA256);
		ctx.update(content);
		Id(ctx.finish().as_ref().try_into().unwrap())
	}
}
