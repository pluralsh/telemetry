//! Reading storage shards that other reader groups serve.
//!
//! A reader serving one shard coordinates a query over all of them: its own
//! shard is read in process and every other shard through a
//! [`LogsShardClient`], the transport the server provides. A remote shard
//! runs the query's pipelines and returns what they kept, which merges with
//! the local shard's exactly as shards merge in process before evaluation.

use std::sync::Arc;

use async_trait::async_trait;
use proto::logs::internal::v1 as pb;
use sharding::ShardId;

use crate::{
    Error, Label, Labels, Namespace, QueryOptions, QueryRequest, Result, query::Direction,
};

/// Transport to the reader groups serving other shards.
#[async_trait]
pub trait LogsShardClient: Send + Sync + 'static {
    /// Every message of the shard's streamed query response.
    async fn query(&self, request: pb::QueryShardRequest) -> Result<Vec<pb::ShardOutput>>;
    async fn label_names(&self, request: pb::LabelNamesRequest) -> Result<pb::StringsResponse>;
    async fn label_values(&self, request: pb::LabelValuesRequest) -> Result<pb::StringsResponse>;
    async fn series(&self, request: pb::SeriesRequest) -> Result<pb::SeriesResponse>;
    async fn segment_generations(
        &self,
        request: pb::SegmentGenerationsRequest,
    ) -> Result<pb::SegmentGenerationsResponse>;
}

pub(crate) fn shard_read(shard: ShardId, namespace: &Namespace) -> pb::ShardRead {
    pb::ShardRead {
        shard: shard.get(),
        namespace: Some(pb::Namespace {
            name: namespace.to_string(),
        }),
    }
}

pub(crate) fn query_request(
    shard: ShardId,
    namespace: &Namespace,
    request: &QueryRequest,
    options: &QueryOptions,
) -> pb::QueryShardRequest {
    pb::QueryShardRequest {
        read: Some(shard_read(shard, namespace)),
        query: request.query.clone(),
        start_ns: request.start_ns,
        end_ns: request.end_ns,
        step_ns: request.step_ns,
        limit: options.limit as u64,
        backward: options.direction == Direction::Backward,
        max_pages: options.max_pages as u64,
        max_concurrency: options.max_concurrency as u64,
        max_in_flight_bytes: options.max_in_flight_bytes as u64,
    }
}

pub(crate) fn query_from_wire(request: &pb::QueryShardRequest) -> (QueryRequest, QueryOptions) {
    let size = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);
    (
        QueryRequest {
            query: request.query.clone(),
            start_ns: request.start_ns,
            end_ns: request.end_ns,
            step_ns: request.step_ns,
        },
        QueryOptions {
            limit: size(request.limit),
            direction: if request.backward {
                Direction::Backward
            } else {
                Direction::Forward
            },
            max_pages: size(request.max_pages),
            max_concurrency: size(request.max_concurrency).max(1),
            max_in_flight_bytes: size(request.max_in_flight_bytes).max(1),
        },
    )
}

pub(crate) fn labels_to_wire(labels: &Labels) -> pb::LabelSet {
    pb::LabelSet {
        labels: labels
            .iter()
            .map(|label| pb::Label {
                name: label.name.clone(),
                value: label.value.clone(),
            })
            .collect(),
    }
}

pub(crate) fn labels_from_wire(labels: pb::LabelSet) -> Result<Labels> {
    Labels::new(
        labels
            .labels
            .into_iter()
            .map(|label| Label::new(label.name, label.value))
            .collect(),
    )
}

/// One shard read through the reader group serving it.
#[derive(Clone)]
pub(crate) struct RemoteShard {
    pub(crate) shard: ShardId,
    pub(crate) client: Arc<dyn LogsShardClient>,
}

impl RemoteShard {
    pub(crate) async fn segment_generations(
        &self,
        namespace: &Namespace,
        starts: &[i64],
    ) -> Result<Vec<Option<u64>>> {
        let response = self
            .client
            .segment_generations(pb::SegmentGenerationsRequest {
                read: Some(shard_read(self.shard, namespace)),
                segment_starts: starts.to_vec(),
            })
            .await?;
        if response.generations.len() != starts.len() {
            return Err(Error::Unavailable(
                "shard reader returned the wrong number of segment generations".to_owned(),
            ));
        }
        Ok(response
            .generations
            .into_iter()
            .map(|generation| generation.value)
            .collect())
    }

    pub(crate) async fn label_names(
        &self,
        namespace: &Namespace,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        Ok(self
            .client
            .label_names(pb::LabelNamesRequest {
                read: Some(shard_read(self.shard, namespace)),
                start_ns,
                end_ns,
            })
            .await?
            .values)
    }

    pub(crate) async fn label_values(
        &self,
        namespace: &Namespace,
        name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<String>> {
        Ok(self
            .client
            .label_values(pb::LabelValuesRequest {
                read: Some(shard_read(self.shard, namespace)),
                name: name.to_owned(),
                start_ns,
                end_ns,
            })
            .await?
            .values)
    }

    pub(crate) async fn series(
        &self,
        namespace: &Namespace,
        selectors: &[String],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<Labels>> {
        self.client
            .series(pb::SeriesRequest {
                read: Some(shard_read(self.shard, namespace)),
                selectors: selectors.to_vec(),
                start_ns,
                end_ns,
            })
            .await?
            .series
            .into_iter()
            .map(labels_from_wire)
            .collect()
    }
}

pub(crate) fn namespace_from_wire(read: Option<&pb::ShardRead>) -> Result<(ShardId, Namespace)> {
    let read = read.ok_or_else(|| Error::Query("missing shard read".into()))?;
    let name = read
        .namespace
        .as_ref()
        .map(|namespace| namespace.name.clone())
        .unwrap_or_default();
    let namespace = Namespace::new(name).map_err(|error| Error::Query(error.to_string()))?;
    Ok((ShardId::new(read.shard), namespace))
}
