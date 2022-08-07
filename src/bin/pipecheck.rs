use std::env::args;
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use futures::stream::StreamExt;

use borgrdr::diag;
use borgrdr::diag::{DiagnosticsSink, Progress};
use borgrdr::fs_store::FsStore;
use borgrdr::repository::Repository;
use borgrdr::rpc::{spawn_rpc_server, RpcStoreClient};
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
		println!("{}[{}]: {}", level, subsystem, message);
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

#[tokio::main]
async fn main() -> Result<()> {
	let argv: Vec<String> = args().collect();

	let (mut segment_meter, mut archive_meter) = split_meter(ProgressMeter::new().shareable());

	let store = Arc::new(FsStore::open(argv[1].clone())?);
	let (serverside, clientside) = tokio::net::UnixStream::pair()?;
	let server = spawn_rpc_server(store, serverside);
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
	for (name, archive_hdr) in manifest.archives().iter() {
		archive_meter.progress(Progress::Range {
			cur: narchives,
			max: manifest.archives().len() as u64,
		});
		narchives += 1;
		let archive_meta = repo.read_archive(archive_hdr.id()).await.with_context(|| {
			// let chunk = repo.store().retrieve(archive_hdr.id()).ok();
			format!("failed to open archive {} @ {:?}", name, archive_hdr.id())
		})?;
		let ids: Vec<_> = archive_meta.items().iter().map(|x| *x).collect();
		let mut archive_item_stream = repo.archive_items(ids)?;
		let mut idbuf = Vec::new();
		while let Some(item) = archive_item_stream.next().await {
			let item =
				item.with_context(|| format!("failed to read archive item from {}", name))?;
			idbuf.extend(item.chunks().iter().map(|x| x.id()));
			if idbuf.len() >= 128 {
				let ids = idbuf.split_off(0);
				assert!(ids.len() > 0);
				for missing_id in repo.store().find_missing_chunks(ids).await? {
					eprintln!("chunk {:?} is missing", missing_id);
				}
			}
			//println!("checked chunks");
			nchunks += item.chunks().len();
			nitems += 1;
		}

		if idbuf.len() > 0 {
			for missing_id in repo.store().find_missing_chunks(idbuf).await? {
				eprintln!("chunk {:?} is missing", missing_id);
			}
		}
	}
	archive_meter.progress(Progress::Complete);

	checker.await??;
	drop(repo);
	server.await?;
	println!(
		"checked {} archives, {} items, {} chunks",
		narchives, nitems, nchunks
	);
	Ok(())
}
