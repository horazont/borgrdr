use std::io;

// start out with 8MiB
static BUFFER_GUESS: usize = 8 * 1024 * 1024 + 8 * 1024 * 128;
// never decompress anything larger than 128 MiB
static MAX_OBJECT_SIZE: usize = 128 * 1024 * 1024;

pub trait Compressor {
	// fn compress(&self, src: &[u8]) -> Vec<u8>;
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>>;
}

pub struct Uncompressed();

impl Uncompressed {
	fn check(header: &[u8]) -> bool {
		if header.len() < 2 {
			return false;
		}
		header[0] == 0x00 && header[1] == 0x00
	}

	fn detect(header: &[u8]) -> Option<Self> {
		if Self::check(header) {
			Some(Self())
		} else {
			None
		}
	}
}

impl Compressor for Uncompressed {
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		if !Self::check(src) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"missing \\x00\\x00 leader for uncompressed data",
			));
		}
		Ok(src[2..].to_vec())
	}
}

pub struct Lz4();

impl Lz4 {
	fn check(header: &[u8]) -> bool {
		if header.len() < 2 {
			return false;
		}
		header[0] == 0x01 && header[1] == 0x00
	}

	fn detect(header: &[u8]) -> Option<Self> {
		if Self::check(header) {
			Some(Self())
		} else {
			None
		}
	}
}

impl Compressor for Lz4 {
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		if !Self::check(src) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"missing \\x01\\x00 leader for lz4 compressed data",
			));
		}
		// strip off id bytes
		let src = &src[2..];
		let mut buffer = Vec::new();
		let guesstimate = if src.len() * 3 >= BUFFER_GUESS {
			src.len() * 3
		} else {
			BUFFER_GUESS
		};
		buffer.resize(guesstimate, 0);

		loop {
			match lz4::block::decompress_to_buffer(src, Some(buffer.len() as i32), &mut buffer[..])
			{
				Ok(sz) => {
					buffer.truncate(sz);
					return Ok(buffer);
				}
				Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
					if buffer.len() >= MAX_OBJECT_SIZE {
						return Err(e);
					}
					buffer.resize(buffer.len() << 1, 0);
				}
				Err(e) => return Err(e),
			}
		}
	}
}

macro_rules! try_compressor {
	($data:ident, $x:ty) => {
		if let Some(c) = <$x>::detect($data) {
			return Some(Box::new(c));
		}
	};
}

pub fn detect_compression(data: &[u8]) -> Option<Box<dyn Compressor>> {
	try_compressor!(data, Lz4);
	try_compressor!(data, Uncompressed);
	None
}
