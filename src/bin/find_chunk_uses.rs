use std::env::args;

use anyhow::{Context, Result};

use futures::stream::StreamExt;

use borgrdr::{segments::Id, store::ObjectStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();
	let argv: Vec<String> = args().collect();

	let (mut backend, repo) = borgrdr::cliutil::open_url_str(&argv[1])
		.await
		.with_context(|| format!("failed to open repository"))?;
	let needle: Id = argv
		.get(2)
		.expect("second argument")
		.parse()
		.expect("not a valid chunk id");
	if !repo.store().contains(needle).await? {
		eprintln!("{:?} not in repository", needle);
		std::process::exit(2);
	}

	let manifest = repo.manifest();
	for v in manifest.archives().values() {
		let archive = repo.read_archive(v.id()).await?;
		if v.id() == &needle {
			println!("archive {:?} is described by {:?}", archive.name(), needle);
		}
		if archive.items().iter().any(|id| id == &needle) {
			println!("the item stream of {:?} uses {:?}", archive.name(), needle);
		}
		let mut archive_item_stream =
			repo.archive_items(archive.items().iter().map(|x| *x).collect())?;
		while let Some(item) = archive_item_stream.next().await {
			let item = item?;
			if item.chunks().iter().any(|chunk| chunk.id() == &needle) {
				println!(
					"{:?} in archive {:?} uses {:?}",
					item.path(),
					archive.name(),
					needle
				);
			}
		}
	}

	/* if let Some(chunks) = chunks_to_extract {
		let mut stream = repo.open_stream(chunks)?;
		tokio::io::copy(&mut stream, &mut tokio::io::stdout()).await?;
	} */

	drop(repo);
	let _: Result<_, _> = backend.wait_for_shutdown().await;
	Ok(())
}
