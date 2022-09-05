use std::env::args;

use anyhow::{Context, Result};

use futures::stream::StreamExt;

use chrono::{DateTime, TimeZone, Utc};

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

	let file_name = &argv[2];
	let manifest = repo.manifest();
	for (_, v) in manifest.archives().iter() {
		let archive = repo.read_archive(v.id()).await?;
		let mut archive_item_stream =
			repo.archive_items(archive.items().iter().map(|x| *x).collect())?;
		while let Some(item) = archive_item_stream.next().await {
			let item = item?;
			let path_str = match std::str::from_utf8(item.path()) {
				Ok(v) => v,
				Err(_) => continue,
			};
			if path_str.contains(&*file_name) {
				print!("{}", archive.name());
				print!("    {} ", mode_to_str(item.mode()));
				if let Some(sz) = item.size() {
					print!(" {:>9}", sz)
				} else {
					print!(" {:>9}", "")
				};
				print!(" {:>5} {:>5}", item.uid(), item.gid());
				if let Some(mtime) = item.mtime() {
					print!(" {:>9}", convert_ts(mtime));
				} else {
					print!(" {:>9}", "");
				}
				print!(" {:?}", item.path());
				println!();
			}
		}
	}

	drop(repo);
	let _: Result<_, _> = backend.wait_for_shutdown().await;
	Ok(())
}
