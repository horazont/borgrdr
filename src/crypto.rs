use std::io;
use std::num::NonZeroU32;
use std::sync::Mutex;

use ring::digest::{Context as DigestContext, SHA256};

use bytes::{Bytes, BytesMut};

use serde::Deserialize;
use serde_with::{serde_as, Bytes as SerdeAsBytes};

use super::aes_ctr;
use super::segments::Id;

pub trait SecretProvider {
	fn encrypted_key(&self) -> io::Result<Bytes>;
	fn passphrase(&self) -> io::Result<Bytes>;
}

pub struct Context {
	repokey_keys: Mutex<Option<Keys>>,
	keyfile_keys: Mutex<Option<Keys>>,
}

impl Context {
	pub fn new() -> Self {
		Self {
			repokey_keys: Mutex::new(None),
			keyfile_keys: Mutex::new(None),
		}
	}

	fn get_repokey_keys(&self, provider: &impl SecretProvider) -> io::Result<Keys> {
		let mut repokey_lock = self.repokey_keys.lock().unwrap();
		if let Some(keys) = repokey_lock.as_ref() {
			return Ok(keys.clone());
		}

		let keybox = provider.encrypted_key()?;
		let passphrase = provider.passphrase()?;
		let keys = decrypt_keybox(keybox, passphrase)?;
		*repokey_lock = Some(keys.clone());
		Ok(keys)
	}

	fn get_keyfile_keys(&self, provider: &impl SecretProvider) -> io::Result<Keys> {
		let path = match std::env::var("BORG_KEYFILE") {
			Ok(v) => v,
			Err(_) => {
				return Err(io::Error::new(
					io::ErrorKind::NotFound,
					"BORG_KEYFILE not set, but keyfile crypto requested",
				))
			}
		};
		todo!();
	}
}

#[serde_as]
#[derive(Deserialize, Debug)]
struct Keybox {
	version: u32,
	#[serde_as(as = "SerdeAsBytes")]
	salt: [u8; 32],
	iterations: NonZeroU32,
	algorithm: String,
	hash: Bytes,
	data: Bytes,
}

impl Keybox {
	fn derive_kek(&self, passphrase: &[u8]) -> Bytes {
		let mut buffer = Vec::new();
		buffer.resize(32, 0);
		ring::pbkdf2::derive(
			ring::pbkdf2::PBKDF2_HMAC_SHA256,
			self.iterations,
			&self.salt,
			passphrase,
			&mut buffer[..],
		);
		buffer.into()
	}

	pub fn extract(self, passphrase: &[u8]) -> io::Result<Bytes> {
		let kek = self.derive_kek(passphrase);
		let mut data: BytesMut = (&self.data[..]).into();
		aes_ctr::apply_ctr_int(&kek[..], 0, &mut data[..]);
		let mut data_digest =
			ring::hmac::Context::with_key(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &kek[..]));
		data_digest.update(&data[..]);
		let data_digest = data_digest.sign();
		match ring::constant_time::verify_slices_are_equal(data_digest.as_ref(), &self.hash[..]) {
			Ok(_) => Ok(data.freeze()),
			Err(_) => Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"passphrase incorrect or corrupted keybox",
			)),
		}
	}
}

#[derive(Deserialize, Debug)]
struct KeyboxInner {
	version: u32,
	repository_id: Bytes,
	enc_key: Bytes,
	enc_hmac_key: Bytes,
	id_key: Bytes,
	chunk_seed: i32,
}

#[derive(Clone)]
struct Keys {
	encryption_key: Bytes,
	encryption_hmac_key: ring::hmac::Key,
	id_hmac_key: Bytes,
}

/// Decrypt a base64-encoded keybox and extract the keys
fn decrypt_keybox(keybox: Bytes, passphrase: Bytes) -> io::Result<Keys> {
	let keybox = match base64::decode(&keybox) {
		Ok(v) => v,
		Err(e) => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"invalid base64 in keybox",
			))
		}
	};
	let keybox: Keybox = rmp_serde::from_read(&mut &keybox[..])
		.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))?;
	assert_eq!(keybox.algorithm, "sha256");
	let keybox_inner = keybox.extract(&passphrase[..])?;
	let keybox_inner: KeyboxInner = rmp_serde::from_read(&mut &keybox_inner[..])
		.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))?;
	Ok(Keys {
		encryption_key: keybox_inner.enc_key,
		encryption_hmac_key: ring::hmac::Key::new(
			ring::hmac::HMAC_SHA256,
			&keybox_inner.enc_hmac_key[..],
		),
		id_hmac_key: keybox_inner.id_key,
	})
}

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
	fn decrypt(&self, src: &[u8]) -> io::Result<Vec<u8>>;
	fn encrypt(&self, src: &[u8]) -> io::Result<Vec<u8>>;
}

impl<T: Key> Key for &T {
	fn decrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		(**self).decrypt(src)
	}

	fn encrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		(**self).encrypt(src)
	}
}

impl<T: Key> Key for Box<T> {
	fn decrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		(**self).decrypt(src)
	}

	fn encrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		(**self).encrypt(src)
	}
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
	fn decrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		assert!(src[0] == 0x02);
		Ok(src[1..].to_vec())
	}

	fn encrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		let mut result = Vec::with_capacity(src.len() + 1);
		result.push(0x02);
		result.copy_from_slice(src);
		Ok(result)
	}
}

impl ContentHasher for Plaintext {
	fn id(&self, content: &[u8]) -> Id {
		let mut ctx = DigestContext::new(&SHA256);
		ctx.update(content);
		Id(ctx.finish().as_ref().try_into().unwrap())
	}
}

pub struct Aes {
	keys: Keys,
}

impl Aes {
	fn new(keys: Keys) -> Self {
		Self { keys }
	}
}

impl Key for Aes {
	fn decrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		assert!(src[0] == 0x03 || src[0] == 0x01);
		assert!(src.len() >= 41);
		let mac = &src[1..33];
		let iv = &src[33..41];

		let mut check_hmac = ring::hmac::Context::with_key(&self.keys.encryption_hmac_key);
		check_hmac.update(&src[33..]);
		let tag = check_hmac.sign();

		match ring::constant_time::verify_slices_are_equal(tag.as_ref(), &mac[..]) {
			Ok(_) => (),
			Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "corrupt mac")),
		};

		let mut full_iv = [0u8; 16];
		full_iv[8..].copy_from_slice(iv);

		let mut result = Vec::new();
		result.extend_from_slice(&src[41..]);
		aes_ctr::apply_ctr(&self.keys.encryption_key[..], &full_iv[..], &mut result[..]);
		Ok(result)
	}

	fn encrypt(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		todo!();
	}
}

pub fn detect_crypto<P: SecretProvider>(
	context: &Context,
	data: &[u8],
	secret_provider: &P,
) -> io::Result<Box<dyn Key>> {
	match data[0] {
		0x03 => {
			Ok(Box::new(Aes::new(context.get_repokey_keys(secret_provider)?)))
		}
		0x02 => Ok(Box::new(Plaintext::new())),
		other => Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported crypto type: {:?}", other),
		)),
	}
}
