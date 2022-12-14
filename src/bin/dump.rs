use std::env::args;

use anyhow::{Context, Result};

use futures::stream::StreamExt;

use chrono::{DateTime, TimeZone, Utc};

use borgrdr::segments::Id;

fn mode_to_str(mode: u32) -> String {
	let mut buf = String::with_capacity(10);
	buf.push('?');
	let mut shift = 9;
	while shift > 0 {
		shift -= 3;
		let bits = (mode >> shift) & 0o7;
		buf.push(if bits & 0o4 != 0 { 'r' } else { '-' });
		buf.push(if bits & 0o2 != 0 { 'w' } else { '-' });
		buf.push(if bits & 0o1 != 0 { 'x' } else { '-' });
	}
	buf
}

fn convert_ts(ts: i64) -> DateTime<Utc> {
	let secs = ts / 1000000000;
	let nanos = (ts % 1000000000) as u32;
	Utc.timestamp(secs, nanos)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();
	let argv: Vec<String> = args().collect();

	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&argv[1])
		.await
		.with_context(|| format!("failed to open repository"))?;

	let archive_name = argv.get(2);
	let file_name = argv.get(3);
	let mut chunks_to_extract: Option<Vec<Id>> = None;
	let manifest = repo.manifest();
	eprintln!("Repository manifest:");
	eprintln!("  Version   : {}", manifest.version());
	eprintln!("  Timestamp : {}", manifest.timestamp());
	eprintln!("  Item Keys : {}", manifest.item_keys().join(", "));
	eprintln!("\nArchives:");
	for (k, v) in manifest.archives().iter() {
		let archive_matches = archive_name.as_ref().map(|x| &k == x).unwrap_or(false);
		eprintln!("{}  [{}] id={:?}", k, v.timestamp(), v.id());
		let archive = repo.read_archive(v.id()).await?;
		eprintln!("  Version: {}", archive.version());
		eprintln!("  Name: {:?}", archive.name());
		eprintln!("  Hostname: {:?}", archive.hostname());
		eprintln!("  Username: {:?}", archive.username());
		eprintln!("  Start time: {:?}", archive.start_time());
		eprintln!("  End time: {:?}", archive.end_time());
		eprintln!("  Comment: {:?}", archive.comment());
		eprintln!("  Items:");
		let mut archive_item_stream =
			repo.archive_items(archive.items().iter().map(|x| *x).collect())?;
		while let Some(item) = archive_item_stream.next().await {
			let item = item?;
			eprint!("    {} ", mode_to_str(item.mode()));
			if let Some(sz) = item.size() {
				eprint!(" {:>9}", sz)
			} else {
				eprint!(" {:>9}", "")
			};
			eprint!(" {:>5} {:>5}", item.uid(), item.gid());
			if let Some(mtime) = item.mtime() {
				eprint!(" {:>9}", convert_ts(mtime));
			} else {
				eprint!(" {:>9}", "");
			}
			eprint!(" {:?}", item.path());
			eprintln!();
			if archive_matches {
				let path_matches = file_name
					.as_ref()
					.map(|x| item.path() == x)
					.unwrap_or(false);
				if path_matches {
					chunks_to_extract = Some(item.chunks().iter().map(|x| *x.id()).collect());
				}
			}
		}
	}

	if let Some(chunks) = chunks_to_extract {
		let mut stream = repo.open_stream(chunks)?;
		tokio::io::copy(&mut stream, &mut tokio::io::stdout()).await?;
	}

	drop(repo);
	let _: Result<_, _> = backend.wait_for_shutdown().await;
	Ok(())
}
