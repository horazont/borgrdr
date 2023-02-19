# `borgrdr`

This is a tool which can extract files from (some) repositories created with [BorgBackup](https://borgbackup.readthedocs.io/).
The tool has been written from scratch, in Rust,
and with no dependencies against any of the original BorgBackup code.

## Why?!

I trust Borg with all my data.
Ever since I started using it
(coming from an rsnapshot-like setup),
I felt a bit uneasy about the fact that I can't just copy files from it to restore my backups.

With this project, I gain(ed) an in-depth understanding of the Borg datastructures.
I am now confident that I can restore data from a (non-corrupt) Borg repository.
And I'm a bit more confident than I was before that I might be able to recover data from a corrupted repository even.

## Limitations

This tool is for entertainment/learning/experimentation purposes **only**.
Even though it (as of the time of writing) only ever opens stuff read-only,
you should probably review the code before letting it loose on your backups.

In addition, the following Borg features are not compatible:

- Repositories touched with Borg 2.x
- Encryption (except repokey, and you need to edit the config to remove the newlines from the key there unless you use any of the `+borg` URL types (see below))

## Usage

To access encrypted repositories,
export the `BORG_PASSPHRASE` environment variable.
Only repokey repositories are supported.

### Interactive browser

```console
$ cargo run --release --bin du --features cursive /path/to/your/repo
```

The `/path/to/your/repo` may either be a relative path, or:

- An absolute `file://` URL
- An absolute `file+borg://` URL,
  to use borg serve to access the local repository.
  Requires a working borg 1.2 installation.
- A `ssh://` URL.
  Requires `BORGRDR_REMOTE_COMMAND`
  to be set to the remote path
  where the `serve` binary can be found
  (build it using `cargo build --release --bin serve`).
- A `ssh+borg://` URL.
  Accesses the repository through `borg serve`
  on the remote side.
  Requires a working borg 1.2 installation on the *remote* side.
  `BORG_REMOTE_COMMAND` is honoured.

### Dumping contents / files

```console
$ cargo run --release --bin dump /path/to/your/repo
```

will print a listing of all archives and files within them,
in snail tempo.

```console
$ cargo run --release --bin dump /path/to/your/repo archivename filepath
```

will first print the listing (to stderr)
and then the contents of the file referenced by `archivename` and `filepath` to stdout.

If you point this at a directory or symlink… no idea what happens, try it! :-)
