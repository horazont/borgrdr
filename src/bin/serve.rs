use std::env::args;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use borgrdr::fs_store::FsStore;
use borgrdr::rpc::spawn_rpc_server;

pin_project_lite::pin_project! {
	struct Stdio {
		#[pin]
		stdin: tokio::io::Stdin,
		#[pin]
		stdout: tokio::io::Stdout,
	}
}

impl AsyncRead for Stdio {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		self.project().stdin.poll_read(cx, buf)
	}
}

impl AsyncWrite for Stdio {
	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().stdout.poll_flush(cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().stdout.poll_shutdown(cx)
	}

	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.project().stdout.poll_write(cx, buf)
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();
	let argv: Vec<String> = args().collect();

	let store = Arc::new(FsStore::open(argv[1].clone())?);
	let stdio = Stdio {
		stdin: tokio::io::stdin(),
		stdout: tokio::io::stdout(),
	};
	Ok(spawn_rpc_server(store, stdio).await?)
}
