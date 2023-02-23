//! Backend for accessing a borg repository
use std::future::Future;
use std::io;
use std::sync::Arc;

use bytes::Bytes;

use super::diag::DiagnosticsSink;
use super::segments::Id;

pub enum PrefetchStreamItem<M, D> {
	Metadata(M),
	Data(io::Result<D>),
}

#[async_trait::async_trait]
pub trait ObjectStore {
	type RetrieveFut<'x>: 'x + Future<Output = io::Result<Bytes>> + Send
	where
		Self: 'x;

	fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> Self::RetrieveFut<'_>;
	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool>;
	async fn find_missing_objects(&self, ids: Vec<Id>) -> io::Result<Vec<Id>>;
	async fn get_repository_config_key(&self, key: &str) -> io::Result<Option<String>>;
	async fn check_all_segments(
		&self,
		progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()>;
}

#[async_trait::async_trait]
impl<S: ObjectStore + Send + Sync> ObjectStore for Arc<S> {
	type RetrieveFut<'x> = S::RetrieveFut<'x>
		where Self: 'x;

	fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> Self::RetrieveFut<'_> {
		(**self).retrieve(id)
	}

	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool> {
		(**self).contains(id).await
	}

	async fn find_missing_objects(&self, ids: Vec<Id>) -> io::Result<Vec<Id>> {
		(**self).find_missing_objects(ids).await
	}

	async fn get_repository_config_key(&self, key: &str) -> io::Result<Option<String>> {
		(**self).get_repository_config_key(key).await
	}

	async fn check_all_segments(
		&self,
		progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()> {
		(**self).check_all_segments(progress).await
	}
}
