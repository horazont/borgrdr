use std::env::args;
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use log::{error, log};

use borgrdr::diag;
use borgrdr::diag::{DiagnosticsSink, Progress};
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

	fn clear(&self) {
		print!("\x1b[K");
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

#[tokio::main]
async fn main() -> Result<()> {
	env_logger::init();
	let argv: Vec<String> = args().collect();

	let meter = ProgressMeter::new().shareable();
	let (mut segment_meter, mut archive_meter) = split_meter(Arc::clone(&meter));

	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&argv[1])
		.await
		.with_context(|| format!("failed to open repository"))?;
	let store = repo.store().clone();

	let (repo_check, archive_check) = tokio::join!(
		store.check_all_segments(Some(&mut segment_meter)),
		repo.check_archives(Some(&mut archive_meter)),
	);
	let _ = meter.lock().and_then(|x| Ok(x.clear()));

	let repo_check = repo_check.with_context(|| format!("repository check failed"));
	let archive_check = archive_check.with_context(|| format!("archive check failed"));

	let mut ok = true;
	if let Some(err) = repo_check.err() {
		error!("{}", err);
		ok = false;
	}
	if let Some(err) = archive_check.err() {
		error!("{}", err);
		ok = false;
	}
	drop(repo);
	drop(store);
	let _: Result<_, _> = backend.wait_for_shutdown().await;
	if !ok {
		// we printed errors already, so we skip the result here
		std::process::exit(125);
	}
	Ok(())
}
