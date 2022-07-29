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

pub struct Lzma();

impl Lzma {
	fn check(header: &[u8]) -> bool {
		if header.len() < 2 {
			return false;
		}
		header[0] == 0x02 && header[1] == 0x00
	}

	fn detect(header: &[u8]) -> Option<Self> {
		if Self::check(header) {
			Some(Self())
		} else {
			None
		}
	}
}

impl Compressor for Lzma {
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		if !Self::check(src) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"missing \\x02\\x00 leader for lzma compressed data",
			));
		}
		// strip off id bytes
		let src = &src[2..];
		lzma::decompress(&src[..]).map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
	}
}

pub struct Zlib();

impl Zlib {
	fn check(header: &[u8]) -> bool {
		if header.len() < 2 {
			return false;
		}
		let cmf = header[0];
		let flg = header[1];
		let is_deflate = cmf & 0x0f == 8;
		let check_ok = (cmf as u16 * 256u16 + flg as u16) % 31 == 0;
		check_ok && is_deflate
	}

	fn detect(header: &[u8]) -> Option<Self> {
		if Self::check(header) {
			Some(Self())
		} else {
			None
		}
	}
}

impl Compressor for Zlib {
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		let mut decompressor = flate2::Decompress::new(true);
		let mut buffer = Vec::with_capacity(BUFFER_GUESS);
		loop {
			let cur_slice = &src[decompressor.total_in() as usize..];
			match decompressor.decompress_vec(
				cur_slice,
				&mut buffer,
				flate2::FlushDecompress::Finish,
			) {
				Ok(flate2::Status::StreamEnd) => {
					buffer.shrink_to_fit();
					return Ok(buffer);
				}
				Ok(flate2::Status::Ok) | Ok(flate2::Status::BufError) => {
					if buffer.capacity() >= MAX_OBJECT_SIZE {
						return Err(io::Error::new(
							io::ErrorKind::InvalidData,
							"zlib output too large",
						));
					}
					buffer.reserve(buffer.len());
				}
				Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
			}
		}
	}
}

pub struct Zstd();

impl Zstd {
	fn check(header: &[u8]) -> bool {
		if header.len() < 2 {
			return false;
		}
		header[0] == 0x03 && header[1] == 0x00
	}

	fn detect(header: &[u8]) -> Option<Self> {
		if Self::check(header) {
			Some(Self())
		} else {
			None
		}
	}
}

impl Compressor for Zstd {
	fn decompress(&self, src: &[u8]) -> io::Result<Vec<u8>> {
		if !Self::check(src) {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"missing \\x02\\x00 leader for lzma compressed data",
			));
		}
		// strip off id bytes
		let src = &src[2..];
		zstd::stream::decode_all(&src[..])
			.map_err(|x| io::Error::new(io::ErrorKind::InvalidData, x))
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
	try_compressor!(data, Lzma);
	try_compressor!(data, Uncompressed);
	try_compressor!(data, Zstd);
	try_compressor!(data, Zlib);
	None
}
