use std::env::args;

use borgrdr::repository::Repository;

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let argv: Vec<String> = args().collect();

	let repo = Repository::open(argv[1].clone())?;
	let manifest = repo.read_manifest()?.unwrap();
	println!("Repository manifest:");
	println!("  Version   : {}", manifest.version());
	println!("  Timestamp : {}", manifest.timestamp());
	println!("  Item Keys : {}", manifest.item_keys().join(", "));
	println!("\nArchives:");
	for (k, v) in manifest.archives().iter() {
		println!("{}  [{}] id={:?}", k, v.timestamp(), v.id());
	}
	Ok(())
}
