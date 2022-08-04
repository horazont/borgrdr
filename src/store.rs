//! Backend for accessing a borg repository
use std::io;

use bytes::Bytes;

use super::progress::ProgressSink;
use super::segments::Id;

#[async_trait::async_trait(?Send)]
pub trait ObjectStore {
	async fn retrieve<K: AsRef<Id>>(&self, id: K) -> io::Result<Bytes>;
	async fn contains<K: AsRef<Id>>(&self, id: K) -> io::Result<bool>;
	async fn get_repository_config_key(&self, key: &str) -> Option<String>;
	async fn check_all_segments(&self, progress: Option<&mut dyn ProgressSink>) -> io::Result<()>;
}
