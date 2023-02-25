# `borgrdr`

`borgrdr` is a tool to inspect (some) backup repositories
created with [BorgBackup](https://borgbackup.readthedocs.io/).
The tool shares no code with borgbackup itself,
and has no dependencies against any of the original BorgBackup code.

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
- Keyfile encryption
- When accessing a repository with repokey encryption, it may be necessary to edit the `config`.

## Building

```console
$ cargo build --release --features cursive
```

Omit `--features cursive`
if you are not interested in the terminal UI.

## Usage

To access encrypted repositories,
export the `BORG_PASSPHRASE` environment variable.
Only repokey repositories are supported.

Commands generally operate on a repository,
which needs to be specified via a URL (`REPO_URL`).
The following options exist:

- `file://` URL, relative path, or absolute path:
  The repository is accessed directly.
- `file+borg://` URL.
  In this case,
  `borg serve` is started,
  and all access to the repository goes through `borg serve`.
  No modifications to the repository necessary.
- `ssh://` URL.
  Requires the `serve` binary on the remote server.
  The path to the `serve` binary on the remote server
  must be exported as `BORGRDR_REMOTE_COMMAND` environment variable.
- `ssh+borg://` URL.
  Like with `file+borg://`,
  `borg serve` is used to access the remote repository.
  Thus, borg must be installed and working on the remote server.
  `BORG_REMOTE_PATH` is honoured.

**Note:** When **not** using any of the `+borg` URL variants,
you need to remove the whitespace (including newlines)
from the `key` entry in the repository `config` file,
when accessing repokey repositories.
This is due to a limitation of the crate used to parse the config files.

**Note:** Using `borg serve` is only tested against 1.2.x.

**Note:** If you receive a `manifest inaccessible: invalid base64 in keybox`
error, either:

- Edit the `config` file inside the repository and remove the linebreaks in the
  `key` value, as described above, or
- Switch to a `+borg` URL.

### Interactive browser

```console
$ cargo run --release --bin du --features cursive REPO_URL
```

The view in the interactive browser is split in two halves:

- Top half: file listing of the repository.
  Press enter to enter a directory.
  Press backspace to move up one layer.
- Bottom half:
  The unique versions availabe of the currently selected file/directory.
  Versions which have the same data are deduplicated and not shown.
  Press enter to extract that version.

  **Note:** Currently, the file metadata is not restored
  (timestamps, ownership, modes)
  when extracting.

Use tab to switch focus between the views.
Press F1 to display a description of the table columns.
Mouse interaction supported,
but slightly quirky.

The visualised file tree is merged across all archives,
which may be confusing depending on how you structure your archives.

**Note:**
The time needed to build the tree is proportional to the number of
files and directories in all archives.
The amount of memory needed is proportional to the number of unique file and
directory versions.

**Note:**
After the initial loading phase,
size summaries are still being calculated.
The progress of that is shown on the bottom right,
and you will see size columns showing `??` during that phase.
Unlike the initial loading phase,
you can already use the browser during this phase.

### Dumping contents / files

```console
$ cargo run --release --bin dump REPO_URL
```

will print a listing of all archives and files within them,
in snail tempo.

```console
$ cargo run --release --bin dump REPO_URL archivename filepath
```

will first print the listing (to stderr)
and then the contents of the file referenced by `archivename` and `filepath` to stdout.

If you point this at a directory or symlink… no idea what happens, try it! :-)
