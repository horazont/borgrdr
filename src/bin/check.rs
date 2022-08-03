use std::env::args;
use std::io;
use std::io::Write;

use anyhow::{Context, Result};

use borgrdr::fs_store::FsStore;
use borgrdr::progress::{FnProgress, Progress};
use borgrdr::repository::Repository;
use borgrdr::store::ObjectStore;

fn print_progress(progress: Progress) {
	match progress {
		Progress::Range { cur, max } => {
			let percentage = ((cur as f64) / (max as f64)) * 100.;
			print!("... {:5.1}%\r", percentage);
		}
		Progress::Count(at) => {
			print!("... {:16}\r", at);
		}
		Progress::Ratio(v) => {
			let percentage = v * 100.;
			print!("... {:5.1}%\r", percentage);
		}
		Progress::Complete => {
			print!("\x1b[K");
		}
	}
	let _ = io::stdout().flush();
}

fn main() -> Result<()> {
	let argv: Vec<String> = args().collect();

	let mut progress_sink: FnProgress<_> = print_progress.into();
	let store = FsStore::open(argv[1].clone())?;
	eprintln!("checking segments ...");
	store.check_all_segments(Some(&mut progress_sink))?;
	eprintln!("segment check ok");
	let repo = Repository::open(store, Box::new(borgrdr::repository::EnvPassphrase::new()))
		.with_context(|| "failed to open repository")?;
	let manifest = repo.manifest();
	let mut narchives = 0;
	let mut nitems = 0;
	let mut nchunks = 0;
	for (name, archive_hdr) in manifest.archives().iter() {
		narchives += 1;
		let archive_meta = repo.read_archive(archive_hdr.id()).with_context(|| {
			let chunk = repo.store().retrieve(archive_hdr.id()).ok();
			format!(
				"failed to open archive {} @ {:?}. chunk: {:?}",
				name,
				archive_hdr.id(),
				chunk
			)
		})?;
		for item in repo.archive_items(archive_meta.items().iter()) {
			let item =
				item.with_context(|| format!("failed to read archive item from {}", name))?;
			for chunk in item.chunks().iter() {
				if !repo.store().contains(chunk.id())? {
					eprintln!("chunk {:?} is missing", chunk.id());
				}
			}
			nchunks += item.chunks().len();
			nitems += 1;
		}
	}

	println!(
		"checked {} archives, {} items, {} chunks",
		narchives, nitems, nchunks
	);
	Ok(())
}
