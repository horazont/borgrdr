use std::env::args;
use std::io::Write;

use anyhow::{Context, Result};

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

	let data = repo.store().retrieve(needle).await?;
	std::io::stdout().write_all(&data)?;

	drop(repo);
	let _: Result<_, _> = backend.wait_for_shutdown().await;
	Ok(())
}
