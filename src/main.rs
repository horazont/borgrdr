use std::env::args;

use borgrdr::fs_store::FsStore;
use borgrdr::repository::Repository;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = args().collect();

	let store = FsStore::open(argv[1].clone())?;
	let repo = Repository::open(store)?;
	let manifest = repo.manifest();
	println!("Repository manifest:");
	println!("  Version   : {}", manifest.version());
	println!("  Timestamp : {}", manifest.timestamp());
	println!("  Item Keys : {}", manifest.item_keys().join(", "));
	println!("\nArchives:");
	for (k, v) in manifest.archives().iter() {
		println!("{}  [{}] id={:?}", k, v.timestamp(), v.id());
		let archive = repo.read_archive(v.id())?;
		println!("  Version: {}", archive.version());
		println!("  Name: {:?}", archive.name());
		println!("  Hostname: {:?}", archive.hostname());
		println!("  Username: {:?}", archive.username());
		println!("  Start time: {:?}", archive.start_time());
		println!("  End time: {:?}", archive.end_time());
		println!("  Comment: {:?}", archive.comment());
		println!("  Items: {:?}", archive.items());
	}
	Ok(())
}
