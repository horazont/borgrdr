//! Backend for accessing a borg repository
use std::io;
use std::sync::Arc;

use bytes::Bytes;

use futures::stream::Stream;

use super::diag::DiagnosticsSink;
use super::segments::Id;

#[async_trait::async_trait]
pub trait ObjectStore {
	type ObjectStream: Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static;

	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes>;
	async fn contains<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<bool>;
	async fn find_missing_objects(&self, ids: Vec<Id>) -> io::Result<Vec<Id>>;
	async fn get_repository_config_key(&self, key: &str) -> io::Result<Option<String>>;
	async fn check_all_segments(
		&self,
		progress: Option<&mut (dyn DiagnosticsSink + Send)>,
	) -> io::Result<()>;

	// we have to take Vec<Id> here for the lack of type-GATs
	// this will likely only be implementable on Arc<ObjectStore>,
	// for lack of lifetime-GATs
	fn stream_objects(&self, object_ids: Vec<Id>) -> io::Result<Self::ObjectStream>;
}

#[async_trait::async_trait]
impl<S: ObjectStore + Send + Sync> ObjectStore for Arc<S> {
	type ObjectStream = S::ObjectStream;

	async fn retrieve<K: AsRef<Id> + Send>(&self, id: K) -> io::Result<Bytes> {
		(**self).retrieve(id).await
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

	fn stream_objects(&self, object_ids: Vec<Id>) -> io::Result<Self::ObjectStream> {
		(**self).stream_objects(object_ids)
	}
}
