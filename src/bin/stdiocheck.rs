use std::env::args_os;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use anyhow::{Context as AnyhowContext, Result};

use log::{debug, error, log, trace};

use futures::stream::StreamExt;

use borgrdr::diag;
use borgrdr::diag::{DiagnosticsSink, Progress};
use borgrdr::repository::Repository;
use borgrdr::rpc::RpcStoreClient;
use borgrdr::store::ObjectStore;

struct ProgressMeter {
	meters: [Progress; 2],
}

impl ProgressMeter {
	fn new() -> Self {
		Self {
			meters: [Progress::Ratio(0.); 2],
		}
	}

	fn shareable(self) -> Arc<Mutex<Self>> {
		Arc::new(Mutex::new(self))
	}

	fn format_one(p: &Progress) -> String {
		match p {
			Progress::Range { cur, max } => {
				let percentage = ((*cur as f64) / (*max as f64)) * 100.;
				format!("{:5.1}%", percentage)
			}
			Progress::Count(at) => {
				format!("{}", *at)
			}
			Progress::Ratio(v) => {
				let percentage = *v * 100.;
				format!("{:5.1}%", percentage)
			}
			Progress::Complete => "100.0%".to_string(),
		}
	}

	fn print(&self) {
		let segments = Self::format_one(&self.meters[0]);
		let archives = Self::format_one(&self.meters[1]);
		print!("\x1b[Ksegments: {}  archives: {}\r", segments, archives);
		let _ = io::stdout().flush();
	}
}

struct ProgressMeterRef {
	inner: Arc<Mutex<ProgressMeter>>,
	index: usize,
}

impl DiagnosticsSink for ProgressMeterRef {
	fn progress(&mut self, progress: Progress) {
		let mut lock = match self.inner.lock() {
			Ok(v) => v,
			Err(_) => return,
		};
		lock.meters[self.index] = progress;
		lock.print();
	}

	fn log(&mut self, level: diag::Level, subsystem: &str, message: &str) {
		if level < diag::Level::Warning {
			return;
		}
		log!(level.into(), "{}: {}", subsystem, message);
	}
}

fn split_meter(meter: Arc<Mutex<ProgressMeter>>) -> (ProgressMeterRef, ProgressMeterRef) {
	(
		ProgressMeterRef {
			inner: meter.clone(),
			index: 0,
		},
		ProgressMeterRef {
			inner: meter.clone(),
			index: 1,
		},
	)
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

#[tokio::main]
async fn main() -> Result<()> {
	env_logger::init();
	let mut arg_iter = args_os().skip(1);

	let (mut segment_meter, mut archive_meter) = split_meter(ProgressMeter::new().shareable());

	let mut cmd = tokio::process::Command::new(arg_iter.next().unwrap());
	cmd.args(arg_iter)
		.stdin(std::process::Stdio::piped())
		.stdout(std::process::Stdio::piped());
	let mut server = cmd.spawn()?;
	let clientside = ChildStdio {
		stdin: server.stdin.take().unwrap(),
		stdout: server.stdout.take().unwrap(),
	};

	let store = Arc::new(RpcStoreClient::new(clientside));
	let store_ref = Arc::clone(&store);
	let checker =
		tokio::spawn(async move { store_ref.check_all_segments(Some(&mut segment_meter)).await });
	let repo = Repository::open(store, Box::new(borgrdr::repository::EnvPassphrase::new()))
		.await
		.with_context(|| "failed to open repository")?;
	let manifest = repo.manifest();
	let mut narchives = 0;
	let mut nitems = 0;
	let mut nchunks = 0;
	let mut ok = true;
	for (name, archive_hdr) in manifest.archives().iter() {
		archive_meter.progress(Progress::Range {
			cur: narchives,
			max: manifest.archives().len() as u64,
		});
		narchives += 1;
		let archive_meta = match repo.read_archive(archive_hdr.id()).await {
			Ok(v) => {
				debug!("opened archive {} (id={:?})", name, archive_hdr.id());
				v
			}
			Err(e) => {
				archive_meter.log(
					diag::Level::Error,
					"archives",
					&format!(
						"failed to open archive {} (id={:?}): {}",
						name,
						archive_hdr.id(),
						e
					),
				);
				ok = false;
				continue;
			}
		};
		let ids: Vec<_> = archive_meta.items().iter().map(|x| *x).collect();
		let mut archive_item_stream = repo.archive_items(ids)?;
		let mut idbuf = Vec::new();
		while let Some(item) = archive_item_stream.next().await {
			let item = match item {
				Ok(v) => v,
				Err(e) => {
					archive_meter.log(
						diag::Level::Error,
						"archive_items",
						&format!("failed to read item metadata from {}: {}", name, e),
					);
					ok = false;
					continue;
				}
			};
			trace!("archive {}: found item {:?}", name, item.path());
			idbuf.extend(item.chunks().iter().map(|x| x.id()));
			if idbuf.len() >= 128 {
				let ids = idbuf.split_off(0);
				assert!(ids.len() > 0);
				for missing_id in repo.store().find_missing_objects(ids).await? {
					archive_meter.log(
						diag::Level::Error,
						"archive_items",
						&format!("chunk {:?} is missing", missing_id),
					);
				}
			}
			//println!("checked chunks");
			nchunks += item.chunks().len();
			nitems += 1;
		}

		if idbuf.len() > 0 {
			for missing_id in repo.store().find_missing_objects(idbuf).await? {
				archive_meter.log(
					diag::Level::Error,
					"archive_items",
					&format!("chunk {:?} is missing", missing_id),
				);
			}
		}

		debug!("archive {} checked competely", name);
	}
	archive_meter.progress(Progress::Complete);

	match checker.await? {
		Ok(_) => (),
		Err(e) => {
			error!("repository check failed: {}", e);
			ok = false;
		}
	};
	drop(repo);
	server.wait().await?;
	println!(
		"checked {} archives, {} items, {} chunks",
		narchives, nitems, nchunks
	);
	if ok {
		Ok(())
	} else {
		Err(anyhow::Error::msg("one or more checks failed"))
	}
}
