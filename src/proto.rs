use bytes::Bytes;
use digest::Digest;
use enr::{EnrKey, NodeId};
use ethereum_types::H256;
use futures::SinkExt;
use rand::prelude::*;
use rlp::Rlp;
use rlp_derive::{RlpDecodable as Decodable, RlpEncodable as Encodable};
use sha2::Sha256;
use std::{
    collections::HashMap,
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::BitXor,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    prelude::*,
    stream::{StreamExt, *},
};
use tokio_util::{codec::*, udp::*};

use super::NodeEntry;

const MAX_PACKET_LEN: usize = 1280;

const HANDSHAKE_TIMEOUT: u64 = 500;

#[derive(Decodable, Encodable)]
pub struct Tag(H256);

impl Tag {
    pub fn create(source: H256, destination: H256) -> Self {
        Self(H256(Sha256::digest(&destination.0).into()).bitxor(source))
    }

    pub fn recover_source(&self, destination: H256) -> H256 {
        H256(Sha256::digest(&destination.0).into()).bitxor(self.0)
    }
}

pub struct PacketBase {
    pub tag: H256,
}

#[derive(Encodable)]
pub struct WhoAreYouPacket {
    pub nonce: u64,
    pub enr_seq: u64,
}

enum ServerTrack {
    WhoAreYouSent { nonce: u64 },
}

enum ClientTrack {}

enum PeerState {
    /// Handshake active. We are server.
    HandshakeServer(ServerTrack),
    /// Handshake active. We are client.
    HandshakeClient(ClientTrack),
    /// Ready for new request.
    Ready,
}

pub struct DiscoveryIo<K: EnrKey> {
    _marker: PhantomData<K>,
}

impl<K: EnrKey + 'static> DiscoveryIo<K> {
    pub async fn new<'a>(
        key: K,
        addr: String,
        enr_seqno: impl Fn(H256) -> &'a mut NodeEntry<K> + Send + 'static,
    ) -> Self {
        let DUMMY_ADDR = SocketAddr::from((Ipv4Addr::new(0, 0, 0, 0), 0));

        let host_id = H256(NodeId::from(key.public()).raw());

        let socket = UdpSocket::bind(addr).await.unwrap();
        let mut peers = HashMap::<H256, PeerState>::new();

        let (mut tx, mut rx) = futures::StreamExt::split(UdpFramed::new(socket, BytesCodec::new()));

        let mut pending_whoareyou = HashMap::<SocketAddr, H256>::new();

        let mut timeout_keys = HashMap::<SocketAddr, tokio::time::delay_queue::Key>::new();
        let mut delay_queue = tokio::time::DelayQueue::<SocketAddr>::new();
        // Prime the delay queue so that it always returns `Pending`
        delay_queue.insert(DUMMY_ADDR, Duration::from_secs(u64::max_value()));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Ingress packet handling
                    pkt = rx.try_next() => {
                        if let Ok(Some((bytes, addr))) = pkt {
                            if bytes.len() > MAX_PACKET_LEN {
                                continue;
                            }

                            let bytes = &*bytes;
                            if let Some(peer_id) = pending_whoareyou.remove(&addr).or_else(|| {
                                Some(
                                    Rlp::new(bytes)
                                        .iter()
                                        .next()?
                                        .as_val::<Tag>()
                                        .ok()?
                                        .recover_source(host_id),
                                )
                            }) {
                                // Clear the timeout
                                if let Some(key) = timeout_keys.remove(&addr) {
                                    delay_queue.remove(&key);
                                }

                                let mut new_status = None;
                                match peers.remove(&peer_id) {
                                    None => {
                                        // Unknown node, we are server
                                        let nonce = rand::random();
                                        let enr_seq = (enr_seqno)(peer_id).record.seq();
                                        let bytes = WhoAreYouPacket { nonce, enr_seq };
                                        if tx.send((rlp::encode(&bytes).into(), addr)).await.is_ok() {
                                            new_status = Some((
                                                PeerState::HandshakeServer(ServerTrack::WhoAreYouSent {
                                                    nonce,
                                                }),
                                                HANDSHAKE_TIMEOUT,
                                            ));
                                        }
                                    }
                                    Some(PeerState::HandshakeServer(track)) => match track {
                                        ServerTrack::WhoAreYouSent { nonce } => {
                                            // if let Ok(data) = rlp::decode(bytes) {}
                                        }
                                    },
                                    _ => unimplemented!(),
                                };

                                if let Some((status, timeout)) = new_status {
                                    timeout_keys.insert(
                                        addr,
                                        delay_queue.insert(addr, Duration::from_millis(timeout)),
                                    );
                                    peers.insert(peer_id, status);
                                }
                            }
                        }
                    }
                    timeout = delay_queue.try_next() => {
                        if let Ok(Some(timeout)) = timeout {
                            let addr = timeout.into_inner();
                            // peers.remove(&peer_id);
                            pending_whoareyou.remove(&addr);
                            timeout_keys.remove(&addr);
                        }
                    }
                }
            }
        });

        Self {
            _marker: PhantomData,
        }
    }
}

// struct Discovery {
//     pending_send: (Bytes, SocketAddr),
//     socket: UdpFramed<BytesCodec>,
// }

// impl Stream for Discovery {
//     type Item = (H256, )
// }
