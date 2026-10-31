//! Where the [`Topology`] lives.
//!
//! [`TopologyStore`] is a compare-and-swap register with a watch. Standalone
//! clusters use [`CatalogTopologyStore`], one record in a small database at
//! `{root}/catalog`; Kubernetes deployments use a `VectorTopology` custom
//! resource (see [`kubernetes`](super::kubernetes)).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::Storage;
use common::storage::{PutRecordOp, Record, RecordOp, WriteOptions};
use tokio::sync::{Mutex, watch};

use super::topology::Topology;
use crate::error::{Error, Result};

#[async_trait]
pub trait TopologyStore: Send + Sync {
    /// The stored topology, read through to the backing store.
    async fn load(&self) -> Result<Option<Topology>>;

    /// Replaces the topology if it is still at generation `expected` (`None`:
    /// absent). Returns `false`, changing nothing, when it is not.
    async fn publish(&self, expected: Option<u64>, next: &Topology) -> Result<bool>;

    /// The latest topology seen, updated as it changes.
    fn watch(&self) -> watch::Receiver<Option<Topology>>;

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

const TOPOLOGY_KEY: &str = "topology";

/// A topology kept in the cluster's catalog database. Only one process may
/// open it.
pub(crate) struct CatalogTopologyStore {
    storage: Arc<dyn Storage>,
    lock: Mutex<()>,
    tx: watch::Sender<Option<Topology>>,
}

impl CatalogTopologyStore {
    pub(crate) async fn open(storage: Arc<dyn Storage>) -> Result<Self> {
        let current = read(storage.as_ref()).await?;
        Ok(Self {
            storage,
            lock: Mutex::new(()),
            tx: watch::channel(current).0,
        })
    }
}

async fn read(storage: &dyn Storage) -> Result<Option<Topology>> {
    match storage.get(Bytes::from(TOPOLOGY_KEY)).await? {
        Some(record) => serde_json::from_slice(&record.value)
            .map(Some)
            .map_err(|e| Error::Encoding(format!("decode topology: {e}"))),
        None => Ok(None),
    }
}

#[async_trait]
impl TopologyStore for CatalogTopologyStore {
    async fn load(&self) -> Result<Option<Topology>> {
        Ok(self.tx.borrow().clone())
    }

    async fn publish(&self, expected: Option<u64>, next: &Topology) -> Result<bool> {
        next.validate()?;
        let _lock = self.lock.lock().await;
        if self.tx.borrow().as_ref().map(|t| t.generation) != expected {
            return Ok(false);
        }
        let value = serde_json::to_vec(next)
            .map_err(|e| Error::Internal(format!("encode topology: {e}")))?;
        self.storage
            .apply_with_options(
                vec![RecordOp::Put(PutRecordOp::new(Record::new(
                    Bytes::from(TOPOLOGY_KEY),
                    Bytes::from(value),
                )))],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await?;
        self.tx.send_replace(Some(next.clone()));
        Ok(true)
    }

    fn watch(&self) -> watch::Receiver<Option<Topology>> {
        self.tx.subscribe()
    }

    async fn close(&self) -> Result<()> {
        self.storage.flush().await?;
        self.storage.close().await?;
        Ok(())
    }
}

/// A process-local topology, for tests of multi-process roles.
#[derive(Default)]
pub struct MemoryTopologyStore {
    lock: Mutex<()>,
    tx: watch::Sender<Option<Topology>>,
}

impl MemoryTopologyStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl TopologyStore for MemoryTopologyStore {
    async fn load(&self) -> Result<Option<Topology>> {
        Ok(self.tx.borrow().clone())
    }

    async fn publish(&self, expected: Option<u64>, next: &Topology) -> Result<bool> {
        next.validate()?;
        let _lock = self.lock.lock().await;
        if self.tx.borrow().as_ref().map(|t| t.generation) != expected {
            return Ok(false);
        }
        self.tx.send_replace(Some(next.clone()));
        Ok(true)
    }

    fn watch(&self) -> watch::Receiver<Option<Topology>> {
        self.tx.subscribe()
    }
}
