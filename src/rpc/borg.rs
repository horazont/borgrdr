use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;

use futures::stream::{Stream, StreamExt};
use futures::SinkExt;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes as SerdeBytes};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

use tokio_util::codec;

use crate::diag::{DiagnosticsSink, Progress};
use crate::rmp_codec::AsymMpCodec;
use crate::segments::Id;
use crate::store::ObjectStore;

use super::worker::{
	CompletionStream, CompletionStreamItem, Error, MessageSendError, MessageSender, Result,
	RpcInterfaceError, RpcItem, RpcMessage, RpcRequest, RpcResponse, RpcWorkerCommand,
	RpcWorkerConfig, RpcWorkerMessage, TryMessageSendError,
};

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub(super) enum BorgRpcValue {
	Blob(Bytes),
	Text(String),
	Bool(bool),
	Nil,
}

impl From<Vec<u8>> for BorgRpcValue {
	fn from(other: Vec<u8>) -> Self {
		Self::Blob(other.into())
	}
}

impl From<String> for BorgRpcValue {
	fn from(other: String) -> Self {
		Self::Text(other)
	}
}

impl From<bool> for BorgRpcValue {
	fn from(other: bool) -> Self {
		Self::Bool(other)
	}
}

#[derive(Debug, Serialize)]
pub(super) struct BorgRpcRequest {
	#[serde(rename = "i")]
	msgid: u64,
	#[serde(rename = "m")]
	method: String,
	#[serde(rename = "a")]
	args: HashMap<String, BorgRpcValue>,
}

impl TryFrom<RpcItem> for BorgRpcRequest {
	type Error = RpcInterfaceError;

	fn try_from(other: RpcItem) -> StdResult<Self, Self::Error> {
		let result = match other {
			RpcItem::Hello => Err(Self::Error::UnsupportedIgnore(RpcItem::Hello)),
			RpcItem::Goodbye => Err(Self::Error::UnsupportedIgnore(RpcItem::Goodbye)),
			RpcItem::Message {
				in_reply_to,
				payload,
			} => Err(Self::Error::UnsupportedIgnore(RpcItem::Message {
				in_reply_to,
				payload,
			})),
			RpcItem::Heartbeat => Err(Self::Error::UnsupportedIgnore(RpcItem::Heartbeat)),
			RpcItem::Request { id, payload } => match payload {
				RpcRequest::RetrieveObject { id: object_id } => Ok(BorgRpcRequest {
					msgid: id,
					method: "get".into(),
					args: vec![("id".to_string(), object_id.0[..].to_vec().into())]
						.into_iter()
						.collect(),
				}),
				RpcRequest::GetRepositoryConfigKey { key } => {
					if key == "key" {
						Ok(BorgRpcRequest {
							msgid: id,
							method: "load_key".into(),
							args: HashMap::new(),
						})
					} else {
						Err(Self::Error::UnsupportedFail(RpcItem::Request {
							id,
							payload: RpcRequest::GetRepositoryConfigKey { key },
						}))
					}
				}
				other => Err(Self::Error::UnsupportedFail(RpcItem::Request {
					id,
					payload: other,
				})),
			},
			other => Err(Self::Error::UnsupportedFail(other)),
		};
		if let Ok(v) = result.as_ref() {
			log::trace!("encoded RpcItem as {:?}", v);
		}
		result
	}
}

#[serde_as]
#[derive(Deserialize)]
#[serde(untagged)]
pub(super) enum BorgRpcResponse {
	Exception {
		#[serde(rename = "i")]
		msgid: u64,
		#[serde_as(as = "SerdeBytes")]
		exception_class: Vec<u8>,
		#[serde_as(as = "SerdeBytes")]
		exception_short: Vec<u8>,
	},
	Result {
		#[serde(rename = "i")]
		msgid: u64,
		#[serde(rename = "r")]
		result: BorgRpcValue,
	},
}

impl From<BorgRpcValue> for RpcResponse {
	fn from(other: BorgRpcValue) -> Self {
		match other {
			BorgRpcValue::Blob(v) => Self::DataReply(v.into()),
			// BorgRpcValue::Bytes(v) => Self::DataReply(v.into()),
			BorgRpcValue::Text(v) => Self::DataReply(v.into()),
			BorgRpcValue::Bool(v) => Self::BoolReply(v),
			BorgRpcValue::Nil => Self::Nil,
		}
	}
}

impl From<BorgRpcResponse> for RpcItem {
	fn from(other: BorgRpcResponse) -> Self {
		match other {
			BorgRpcResponse::Exception {
				msgid,
				exception_short,
				..
			} => {
				let exception_short = String::from_utf8_lossy(&exception_short);
				Self::Response {
					id: msgid,
					payload: RpcResponse::Error(exception_short.into_owned()),
				}
			}
			BorgRpcResponse::Result { msgid, result } => Self::Response {
				id: msgid,
				payload: result.into(),
			},
		}
	}
}

macro_rules! match_rpc_response {
	($x:expr => {
		$($p:pat_param => $px:expr,)*
	}) => {
		match $x {
			$($p => $px,)*
			Ok(RpcResponse::Error(e)) => Err(Error::Remote(e)),
			Ok(other) => Err(Error::UnexpectedResponse(other)),
			Err(e) => Err(e),
		}
	}
}

pub(super) async fn borg_open<T: AsyncRead + AsyncWrite + Send + Unpin>(
	ch: &mut codec::Framed<T, AsymMpCodec<BorgRpcRequest, BorgRpcResponse>>,
	repository_path: String,
) -> io::Result<()> {
	ch.send(BorgRpcRequest {
		msgid: 0,
		method: "open".into(),
		args: vec![
			("path".to_string(), repository_path.into()),
			("exclusive".to_string(), false.into()),
		]
		.into_iter()
		.collect(),
	})
	.await?;
	match ch
		.next()
		.await
		.map(|x| x.map(|x: BorgRpcResponse| x.into()))
	{
		Some(Ok(RpcItem::Response { id, payload })) => {
			if id != 0 {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					format!("unexpected rpc id: {:?}", id),
				));
			}
			match payload {
				RpcResponse::Error(e) => Err(Error::Remote(e).into()),
				RpcResponse::DataReply(_) => Ok(()),
				other => Err(Error::UnexpectedResponse(other).into()),
			}
		}
		Some(Ok(other)) => Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unexpected rpc item after open: {:?}", other),
		)),
		Some(Err(e)) => Err(e),
		None => Err(io::Error::new(
			io::ErrorKind::UnexpectedEof,
			"channel closed before reply to open".to_string(),
		)),
	}
}
