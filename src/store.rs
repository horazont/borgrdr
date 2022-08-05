//! Backend for accessing a borg repository
use std::io;
use std::sync::Arc;

use bytes::Bytes;

use super::progress::ProgressSink;
use super::segments::Id;

#[async_trait::async_trait]
pub trait ObjectStore {
	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes>;
	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool>;
	async fn get_repository_config_key(&self, key: &str) -> Option<String>;
	async fn check_all_segments(
		&self,
		progress: Option<&mut (dyn ProgressSink + Send)>,
	) -> io::Result<()>;
}

#[async_trait::async_trait]
impl<S: ObjectStore + Send + Sync> ObjectStore for Arc<S> {
	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes> {
		(**self).retrieve(id).await
	}

	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool> {
		(**self).contains(id).await
	}

	async fn get_repository_config_key(&self, key: &str) -> Option<String> {
		(**self).get_repository_config_key(key).await
	}

	async fn check_all_segments(
		&self,
		progress: Option<&mut (dyn ProgressSink + Send)>,
	) -> io::Result<()> {
		(**self).check_all_segments(progress).await
	}
}
