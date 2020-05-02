#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::default_trait_access,
    clippy::use_self,
    clippy::wildcard_imports
)]

use arr_macro::arr;
use derivative::Derivative;
use enr::*;
use ethereum_types::*;
use futures::{Sink, SinkExt};
use heapless::{
    consts::{U16, U4096},
    FnvIndexMap,
};
use log::*;
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    net::SocketAddr,
    ops::BitXor,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    net::UdpSocket,
    prelude::*,
    stream::{StreamExt, *},
};
use tokio_util::{codec::*, udp::*};

pub mod proto;
pub mod topic;

pub type RawNodeId = [u8; 32];

#[must_use]
pub fn distance(a: H256, b: H256) -> U256 {
    U256::from_big_endian(&a.bitxor(b).0)
}

#[must_use]
pub fn logdistance(a: H256, b: H256) -> Option<usize> {
    for i in (0..H256::len_bytes()).rev() {
        let byte_index = H256::len_bytes() - i - 1;
        let d: u8 = a[byte_index] ^ b[byte_index];
        if d != 0 {
            let high_bit_index = 7 - d.leading_zeros() as usize;
            return Some(i * 8 + high_bit_index);
        }
    }
    None // a and b are equal, so log distance is -inf
}

#[derive(Clone, Copy, Debug)]
pub enum PeerState {
    New,
    Ready,
}

#[derive(Derivative)]
#[derivative(Debug(bound = ""))]
pub struct NodeEntry<K: EnrKey> {
    pub record: Enr<K>,
    pub peer_state: PeerState,
    pub liveness: Option<Instant>,
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
struct Bucket<K: EnrKey> {
    nodes: Box<FnvIndexMap<RawNodeId, NodeEntry<K>, U16>>,
    recently_seen: Option<Enr<K>>,
}

pub struct NodeTable<K: EnrKey> {
    host_id: H256,
    buckets: Box<[Bucket<K>; 256]>,

    all_nodes: Box<heapless::FnvIndexSet<RawNodeId, U4096>>,
}

impl<K: EnrKey> NodeTable<K> {
    #[must_use]
    pub fn new(host_id: H256) -> Self {
        Self {
            host_id,
            buckets: Box::new(arr![Default::default(); 256]),
            all_nodes: Default::default(),
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.all_nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn bucket_idx(&self, node_id: H256) -> Option<usize> {
        logdistance(self.host_id, node_id)
    }

    fn bucket(&mut self, node_id: H256) -> Option<&mut Bucket<K>> {
        Some(&mut self.buckets[self.bucket_idx(node_id)?])
    }

    pub fn node_mut(&mut self, node_id: H256) -> Option<&mut NodeEntry<K>> {
        let bucket = self.bucket(node_id)?;
        bucket.nodes.get_mut(&node_id.0)
    }

    pub fn add_node(&mut self, record: Enr<K>, peer_state: PeerState) {
        let node_id = H256(record.node_id().raw());

        // If we don't have such node already...
        if !self.all_nodes.contains(&node_id.0) {
            // Check that we're not adding self
            if let Some(bucket_idx) = self.bucket_idx(node_id) {
                let bucket = &mut self.buckets[bucket_idx];
                // If there's space, add it...
                if bucket.nodes.len() < bucket.nodes.capacity() {
                    let node_id = node_id.0;

                    let _ = self.all_nodes.insert(node_id);
                    let _ = bucket.nodes.insert(
                        node_id,
                        NodeEntry {
                            record,
                            peer_state,
                            liveness: None,
                        },
                    );
                } else {
                    // ...or if at capacity, update replacement cache instead
                    bucket.recently_seen = Some(record);
                }
            }
        }
    }

    pub fn evict_node(&mut self, node_id: H256) {
        if let Some(bucket_idx) = self.bucket_idx(node_id) {
            let bucket = &mut self.buckets[bucket_idx];
            // If this node actually exists, remove it.
            if bucket.nodes.remove(&node_id.0).is_some() {
                self.all_nodes.remove(&node_id.0);

                // And if there is a replacement, move it into the table
                if let Some(record) = bucket.recently_seen.take() {
                    let node_id = record.node_id().raw();

                    let _ = self.all_nodes.insert(node_id);
                    let _ = bucket.nodes.insert(
                        node_id,
                        NodeEntry {
                            record,
                            peer_state: PeerState::New,
                            liveness: None,
                        },
                    );
                }
            }
        }
    }

    pub fn update_liveness(&mut self, node_id: H256, timestamp: Instant) {
        if let Some(node) = self.node_mut(node_id) {
            node.liveness = Some(timestamp);
        }
    }

    pub fn random_node(&mut self) -> Option<&mut NodeEntry<K>> {
        let node_id = *self
            .all_nodes
            .iter()
            .nth(rand::random::<usize>() % self.all_nodes.len())?;

        Some(
            self.node_mut(H256(node_id))
                .expect("this node always exists at this point; qed"),
        )
    }

    pub fn bucket_nodes(&mut self, logdistance: u8) -> BucketNodes<'_, K> {
        BucketNodes(NodeEntries {
            node_table: self,
            current_bucket: logdistance as usize,
            max_bucket: logdistance as usize,
            current_bucket_remaining: None,
        })
    }

    pub fn closest(&mut self) -> Closest<'_, K> {
        Closest(NodeEntries {
            node_table: self,
            current_bucket: 0,
            max_bucket: 255,
            current_bucket_remaining: None,
        })
    }
}

pub struct BucketNodes<'a, K: EnrKey>(NodeEntries<'a, K>);

impl<'a, K: EnrKey> Iterator for BucketNodes<'a, K> {
    type Item = &'a mut NodeEntry<K>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct Closest<'a, K: EnrKey>(NodeEntries<'a, K>);

impl<'a, K: EnrKey> Iterator for Closest<'a, K> {
    type Item = &'a mut NodeEntry<K>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

struct NodeEntries<'a, K: EnrKey> {
    node_table: &'a mut NodeTable<K>,
    current_bucket: usize,
    max_bucket: usize,
    current_bucket_remaining: Option<Vec<NonNull<NodeEntry<K>>>>,
}

impl<'a, K: EnrKey> Iterator for NodeEntries<'a, K> {
    type Item = &'a mut NodeEntry<K>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let NodeEntries {
                node_table,
                current_bucket,
                max_bucket,
                current_bucket_remaining,
            } = self;

            trace!("Current bucket is {}", *current_bucket);

            let host_id = node_table.host_id;

            if let Some(ptr) = current_bucket_remaining
                .get_or_insert_with(|| {
                    let mut nodes = node_table.buckets[*current_bucket]
                        .nodes
                        .values_mut()
                        .collect::<Vec<_>>();

                    trace!("Nodes before sorting: {:?}", nodes);

                    nodes.sort_by(|a, b| {
                        distance(host_id, H256(b.record.node_id().raw()))
                            .cmp(&distance(host_id, H256(a.record.node_id().raw())))
                    });

                    trace!("Nodes after sorting: {:?}", nodes);

                    nodes.into_iter().map(From::from).collect()
                })
                .pop()
            {
                // Safety: we have exclusive access to underlying node table
                return Some(unsafe { &mut *ptr.as_ptr() });
            }

            if *current_bucket == *max_bucket {
                return None;
            }

            *current_bucket += 1;
            *current_bucket_remaining = None;
        }
    }
}

pub enum DiscoveryRequest {
    Ping,
}

pub enum DiscoveryResponse {
    Pong,
}

pub enum DiscoveryPacket {
    WhoAreYou,
    FindNode,
    Ping,
    Pong,
}

pub enum TableUpdate {
    Added { node_id: H256, addr: SocketAddr },
    Removed { node_id: H256 },
}

#[allow(dead_code)]
pub struct Discovery<K: EnrKey> {
    node_table: Arc<Mutex<NodeTable<K>>>,
    concurrency: usize,
}

impl<K: EnrKey + Send + 'static> Discovery<K> {
    pub async fn new<
        F: Fn(TableUpdate) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    >(
        addr: String,
        host_id: H256,
        on_table_update: F,
    ) -> Self {
        let socket = UdpSocket::bind(addr).await.unwrap();
        let on_table_update = Arc::new(on_table_update);

        // Create a node table
        let node_table = Arc::new(Mutex::new(NodeTable::new(host_id)));

        let (tx, mut rx) = futures::StreamExt::split(UdpFramed::new(socket, BytesCodec::new()));
        // Ougoing router
        let (outgoing_sender, mut rx) = tokio::sync::mpsc::channel::<(H256, DiscoveryPacket)>(1);
        // tokio::spawn(async move {
        //     while let Some((node_id, request)) = rx.next().await {
        //         if let Some(node) = node_table.lock().unwrap().node_mut(node_id) {
        //             if let Some(ip) = node.record.ip() {}
        //         }
        //         let _ = io_tx.send((node_id, request)).await;
        //     }
        // });

        // Liveness check service
        let unanswered_pings = Arc::new(Mutex::new(HashSet::<H256>::new()));
        tokio::spawn({
            let node_table = node_table.clone();
            let unanswered_pings = unanswered_pings.clone();
            async move {
                const PING_TIMEOUT: u64 = 10;
                const SCAN_IN: u64 = 30_000;

                loop {
                    let d = {
                        let node_id = node_table
                            .lock()
                            .unwrap()
                            .random_node()
                            .map(|entry| H256(entry.record.node_id().raw()))
                            .filter(|node| !unanswered_pings.lock().unwrap().contains(node));

                        if let Some(node_id) = node_id {
                            let mut outgoing_sender = outgoing_sender.clone();
                            let on_table_update = on_table_update.clone();
                            let node_table = node_table.clone();
                            let unanswered_pings = unanswered_pings.clone();
                            tokio::spawn(async move {
                                let _ =
                                    outgoing_sender.send((node_id, DiscoveryPacket::Ping)).await;

                                tokio::time::delay_for(std::time::Duration::from_secs(
                                    PING_TIMEOUT,
                                ))
                                .await;

                                if unanswered_pings.lock().unwrap().remove(&node_id) {
                                    node_table.lock().unwrap().evict_node(node_id);
                                    (on_table_update)(TableUpdate::Removed { node_id }).await;
                                }
                            });
                        }
                        tokio::time::delay_for(Duration::from_millis(
                            SCAN_IN / node_table.lock().unwrap().len() as u64,
                        ))
                    };

                    d.await;
                }
            }
        });

        // Incoming router
        // tokio::spawn(async move {
        //     while let Some((node_id, response)) = io_rx.next().await {
        //         match response {
        //             DiscoveryResponse::Pong => {
        //                 unanswered_pings.lock().unwrap().remove(&node_id);
        //             }
        //         }
        //     }
        // });

        Self {
            node_table,
            concurrency: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secp256k1::SecretKey;
    #[test]
    fn test_iterator() {
        let _ = env_logger::try_init();
        let host_id = H256::random();
        let mut table = NodeTable::<SecretKey>::new(host_id);

        for _ in 0..9000 {
            table.add_node(
                EnrBuilder::new("v4")
                    .build(&SecretKey::random(&mut rand::thread_rng()))
                    .unwrap(),
                PeerState::Ready,
            )
        }

        let mut max_distance = U256::zero();
        for entry in table.closest() {
            let dst = distance(host_id, H256(entry.record.node_id().raw()));
            trace!("Computed distance is {}", dst);
            assert!(dst >= max_distance);
            max_distance = dst;
        }
    }
}
