use enr::*;
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    hash::Hash,
    time::Instant,
};

pub type RawNodeId = [u8; 32];

pub struct TopicEntry<K: EnrKey> {
    record: Enr<K>,
}

#[derive(Clone, Debug)]
pub enum TopicReserve {
    Ticket,
    Record,
}

/// Data structure
pub struct TopicTable<Topic: Eq + Hash, K: EnrKey> {
    topics: HashMap<Topic, HashMap<RawNodeId, TopicReserve>>,

    all_enrs: HashMap<RawNodeId, (Enr<K>, usize)>,
    total_count: usize,
}

impl<Topic: Eq + Hash, K: EnrKey> TopicTable<Topic, K> {
    pub fn insert(&mut self, topic: Topic, enr: Enr<K>, mut e: TopicReserve) -> bool {
        let node_id = enr.node_id().raw();

        let mut inserted = false;
        if self.total_count < 15_000 {
            let topic = self.topics.entry(topic).or_default();

            if topic.len() < 100 {
                match topic.entry(node_id) {
                    Entry::Vacant(entry) => {
                        entry.insert(e);
                        inserted = true
                    }
                    Entry::Occupied(mut entry) => {
                        let entry = entry.get_mut();
                        if let TopicReserve::Ticket = entry {
                            std::mem::swap(entry, &mut e);
                            inserted = true;
                        }
                    }
                }
            }
        }

        if inserted {
            self.all_enrs.entry(node_id).or_insert_with(|| (enr, 0)).1 += 1;
            self.total_count += 1;
        }
        inserted
    }

    pub fn remove_node(&mut self, topic: &Topic, node_id: RawNodeId) -> bool {
        if let Some(topic) = self.topics.get_mut(&topic) {
            if topic.remove(&node_id).is_some() {
                self.total_count -= 1;
                match self.all_enrs.entry(node_id) {
                    Entry::Vacant(..) => unreachable!(),
                    Entry::Occupied(mut entry) => {
                        let e = entry.get_mut();
                        e.1 -= 1;
                        let remaining = e.1;
                        if remaining == 0 {
                            entry.remove();
                        }
                        return true;
                    }
                }
            }
        }

        false
    }
}

pub struct TopicService<Topic: Eq + Hash, K: EnrKey> {
    table: TopicTable<Topic, K>,
}
