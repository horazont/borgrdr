use std::env::var_os;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::process::{Child, Command};

use crate::fs_store::FsStore;
use crate::repository::{EnvPassphrase, Repository};
use crate::rpc::{spawn_rpc_server, RpcStoreClient};

pub struct NullBackend();

#[async_trait::async_trait]
pub trait Backend {
	async fn kill(&mut self) -> io::Result<()>;
	async fn wait_for_shutdown(&mut self) -> io::Result<()>;
}

#[async_trait::async_trait]
impl Backend for tokio::task::JoinHandle<()> {
	async fn kill(&mut self) -> io::Result<()> {
		Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"in-process server can't be killed",
		))
	}

	async fn wait_for_shutdown(&mut self) -> io::Result<()> {
		Ok(self.await?)
	}
}

#[async_trait::async_trait]
impl Backend for Child {
	async fn kill(&mut self) -> io::Result<()> {
		self.start_kill()
	}

	async fn wait_for_shutdown(&mut self) -> io::Result<()> {
		self.wait().await?;
		Ok(())
	}
}

#[async_trait::async_trait]
impl Backend for NullBackend {
	async fn kill(&mut self) -> io::Result<()> {
		Ok(())
	}

	async fn wait_for_shutdown(&mut self) -> io::Result<()> {
		Ok(())
	}
}

pub async fn open_local_repository<P: Into<PathBuf>>(
	path: P,
) -> io::Result<(tokio::task::JoinHandle<()>, Repository<RpcStoreClient>)> {
	let store = Arc::new(FsStore::open(path.into())?);
	let (serverside, clientside) = tokio::io::duplex(16 * 1024 * 1024);
	let server = spawn_rpc_server(store, serverside);
	let store = RpcStoreClient::new(clientside);
	Ok((
		server,
		Repository::open(store, Box::new(EnvPassphrase::new())).await?,
	))
}

pin_project_lite::pin_project! {
	struct ChildStdio {
		#[pin]
		stdin: tokio::process::ChildStdin,
		#[pin]
		stdout: tokio::process::ChildStdout,
	}
}

impl AsyncRead for ChildStdio {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		self.project().stdout.poll_read(cx, buf)
	}
}

impl AsyncWrite for ChildStdio {
	fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().stdin.poll_flush(cx)
	}

	fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		self.project().stdin.poll_shutdown(cx)
	}

	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		self.project().stdin.poll_write(cx, buf)
	}
}

async fn open_stdio<S: AsRef<OsStr>, I: IntoIterator<Item = S>>(
	command: impl AsRef<OsStr>,
	argv: I,
) -> io::Result<(Child, ChildStdio)> {
	let mut cmd = Command::new(command.as_ref());
	cmd.args(argv)
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped());
	let mut server = cmd.spawn()?;
	let clientside = ChildStdio {
		stdin: server.stdin.take().unwrap(),
		stdout: server.stdout.take().unwrap(),
	};
	Ok((server, clientside))
}

pub async fn open_stdio_repository<S: AsRef<OsStr>, I: IntoIterator<Item = S>>(
	command: impl AsRef<OsStr>,
	argv: I,
) -> io::Result<(Child, Repository<RpcStoreClient>)> {
	let (server, clientside) = open_stdio(command, argv).await?;
	let store = RpcStoreClient::new(clientside);
	Ok((
		server,
		Repository::open(store, Box::new(EnvPassphrase::new())).await?,
	))
}

pub async fn open_borg_repository(
	repository_path: PathBuf,
) -> io::Result<(Child, Repository<RpcStoreClient>)> {
	let repository_path = repository_path.to_str().expect("valid utf-8 path").to_string();
	let executable = match var_os("BORG_REMOTE_PATH") {
		Some(v) => v,
		None => "borg".into(),
	};
	let argv = vec!["serve"];
	let (server, clientside) = open_stdio(&executable, argv).await?;
	let store = RpcStoreClient::new_borg(clientside, repository_path).await?;
	Ok((server, Repository::open(store, Box::new(EnvPassphrase::new())).await?))
}

pub async fn open_ssh(
	connect: impl AsRef<OsStr>,
	path: impl AsRef<OsStr>,
) -> io::Result<(Child, Repository<RpcStoreClient>)> {
	let executable = match var_os("BORGRDR_REMOTE_COMMAND") {
		Some(v) => v,
		None => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"BORGRDR_REMOTE_COMMAND must be set for SSH operation",
			))
		}
	};
	let argv = vec![connect.as_ref(), executable.as_ref(), path.as_ref()];
	open_stdio_repository("ssh", argv).await
}

pub async fn open_url_str(url: &str) -> io::Result<(Box<dyn Backend>, Repository<RpcStoreClient>)> {
	let url = match url::Url::parse(url) {
		Ok(v) => v,
		Err(e) => {
			let mut path = std::env::current_dir()?;
			path.push(url);
			match url::Url::from_directory_path(path) {
				Ok(v) => v,
				Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
			}
		}
	};

	match url.scheme() {
		"ssh" | "ssh+borg" => {
			let borg_mode = url.scheme().ends_with("+borg");
			let host = match url.host_str() {
				Some(v) => v,
				None => {
					return Err(io::Error::new(
						io::ErrorKind::InvalidInput,
						format!("invalid ssh URL (missing host): {}", url),
					))
				}
			};

			let mut connect_str = String::new();
			connect_str.push_str(url.username());
			if connect_str.len() > 0 {
				connect_str.push_str("@");
			}
			connect_str.push_str(host);

			let (backend, repo) = open_ssh(connect_str, url.path()).await?;
			Ok((Box::new(backend), repo))
		}
		"file+borg" => {
			let path = match url.to_file_path() {
				Ok(v) => v,
				Err(_) => {
					return Err(io::Error::new(
						io::ErrorKind::InvalidInput,
						format!("invalid file URL: {}", url),
					))
				}
			};
			let (backend, repo) = open_borg_repository(path).await?;
			Ok((Box::new(backend), repo))
		}
		"file" => {
			let path = match url.to_file_path() {
				Ok(v) => v,
				Err(_) => {
					return Err(io::Error::new(
						io::ErrorKind::InvalidInput,
						format!("invalid file URL: {}", url),
					))
				}
			};
			let (backend, repo) = open_local_repository(path).await?;
			Ok((Box::new(backend), repo))
		}
		other => Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("unsupported URL scheme: {}", other),
		)),
	}
}
