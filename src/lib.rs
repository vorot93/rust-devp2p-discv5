use enr::*;
use ethereum_types::*;
use heapless::{
    consts::{U16, U4096},
    FnvIndexMap,
};
use std::{ops::BitXor, time::Instant};

type RawNodeId = [u8; 32];

pub fn distance(a: H256, b: H256) -> U256 {
    U256::from_big_endian(&a.bitxor(b).0)
}

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

#[derive(Debug)]
pub struct NodeEntry<K: EnrKey> {
    pub record: Enr<K>,
    pub liveness: Option<Instant>,
}

struct Bucket<K: EnrKey> {
    nodes: FnvIndexMap<RawNodeId, NodeEntry<K>, U16>,
    recently_seen: Option<Enr<K>>,
}

pub struct NodeTable<K: EnrKey> {
    node_id: H256,
    buckets: [Bucket<K>; 256],

    all_nodes: heapless::FnvIndexSet<RawNodeId, U4096>,
}

impl<K: EnrKey> NodeTable<K> {
    fn bucket_idx(&self, node_id: H256) -> Option<usize> {
        logdistance(self.node_id, node_id)
    }

    fn bucket(&mut self, node_id: H256) -> Option<&mut Bucket<K>> {
        Some(&mut self.buckets[self.bucket_idx(node_id)?])
    }

    pub fn node_mut(&mut self, node_id: H256) -> Option<&mut NodeEntry<K>> {
        let bucket = self.bucket(node_id)?;
        bucket.nodes.get_mut(&node_id.0)
    }

    pub fn add_node(&mut self, record: Enr<K>) {
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

    pub fn closest(&mut self) -> NodeEntries<'_, K> {
        NodeEntries {
            node_table: self,
            current_bucket: 0,
            current_bucket_remaining: None,
        }
    }
}

pub struct NodeEntries<'a, K: EnrKey> {
    node_table: &'a mut NodeTable<K>,
    current_bucket: usize,
    current_bucket_remaining: Option<Vec<*mut NodeEntry<K>>>,
}

impl<'a, K: EnrKey> Iterator for NodeEntries<'a, K> {
    type Item = &'a mut NodeEntry<K>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let NodeEntries {
                node_table,
                current_bucket,
                current_bucket_remaining,
            } = self;

            if *current_bucket == node_table.buckets.len() {
                return None;
            }

            if let Some(v) = current_bucket_remaining
                .get_or_insert_with(|| {
                    let nodes = node_table.buckets[*current_bucket]
                        .nodes
                        .values_mut()
                        .map(|value| &mut *value as *mut _)
                        .collect::<Vec<_>>();
                    nodes
                })
                .pop()
            {
                return Some(unsafe { v.as_mut().unwrap() });
            }

            *current_bucket += 1;
            *current_bucket_remaining = None;
        }
    }
}
