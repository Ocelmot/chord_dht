use crate::{
    adaptor::ChordAdaptor,
    associate::AssociateChannel,
    chord_id::ChordId,
    error::{ChordResult, ErrorKind, Problem, ProblemWrap},
    ChordAddress,
};

use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::interval;

use serde::{Deserialize, Serialize};

pub(crate) mod message;

mod state;
use state::ChordState;

use message::Message;
use tracing::{info, instrument};

mod private_ops;
mod public_ops;

use self::message::{Packet, PacketType, PrivateMessage, PublicMessage};

/// An internal id associated with channels to determine the relationship of
/// that channel to the node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessorId<M> {
    /// Represents an internal message that should not be exposed.
    #[serde(skip)]
    Internal,
    /// Represents a connection to or from another node with the provided id.
    Member(M),
    /// A connection that may query and recieve responses, but is not part of
    /// the chord itself and cannot respond to queries or route requests.
    Associate(u32),
}

/// The Chord itself. This struct acts like a builder in that it is created
/// and modified before being consumed by the start method which then
/// returns another type, ChordHandle.
#[derive(Debug)]
pub struct Chord<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> {
    // Core data
    self_addr: A,
    self_id: I,
    predecessor: Option<I>,

    // Connections
    listen_addr: A,
    join_list: Vec<A>,
    adaptor: ADAPTOR,
    members: BTreeMap<I, (A, Sender<PublicMessage<A, I>>)>,
    associates: BTreeMap<u32, Sender<PublicMessage<A, I>>>,
    finger_index: u32,
    next_associate_id: Arc<AtomicU32>,
    join_or_host: bool,

    // Operations channel
    channel_rx: Receiver<(ProcessorId<I>, Message<A, I>)>,
    channel_tx: Sender<(ProcessorId<I>, Message<A, I>)>,

    // Other
    file_path: Option<PathBuf>,
    advert: Option<Vec<u8>>,
}

impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR> {
    /// Creates a new Chord instance with the provided id and address.
    /// The address will be passed to the adaptor to function as the
    /// listen address.
    pub fn new(addr: A, self_id: I) -> ChordResult<Self> {
        let (channel_tx, channel_rx) = channel(50);
        let next_associate_id = Arc::new(AtomicU32::new(1));
        let adaptor = ADAPTOR::new(next_associate_id.clone())
            .problem_wrap(crate::error::ErrorKind::AssociateCreationFailed)?;
        let connections = BTreeMap::new();

        Ok(Chord {
            // Core data
            self_addr: addr.clone(),
            self_id,
            predecessor: None,

            // Connections
            listen_addr: addr,
            join_list: Vec::new(),
            adaptor,
            members: connections,
            associates: BTreeMap::new(),
            finger_index: 1,
            next_associate_id,
            join_or_host: false,

            // Operations channel
            channel_rx,
            channel_tx,

            // Other
            file_path: None,
            advert: None,
        })
    }

    /// Create a new chord node from data stored in a file.
    /// Additionally set the chord to save back to this file.
    pub async fn from_file(path: PathBuf) -> ChordResult<Self> {
        let state = ChordState::from_file(&path).await;

        let mut chord = Chord::new(state.node_addr, state.node_id)?;
        chord.set_file(Some(path));
        chord.set_join_list(state.known_addrs);

        chord.set_listen_addr(state.listen_addr);

        Ok(chord)
    }

    /// Set the chord to save its state in a file located at path
    pub fn set_file(&mut self, path: Option<PathBuf>) {
        self.file_path = path;
    }

    /// Set the chord's self address
    pub fn set_self_addr(&mut self, addr: A) {
        self.self_addr = addr;
    }

    /// Set the chord's listen address
    pub fn set_listen_addr(&mut self, addr: A) {
        self.listen_addr = addr;
    }

    /// Set if this chord will default to hosting if the join attempts fail
    pub fn set_join_or_host(&mut self, join_or_host: bool) {
        self.join_or_host = join_or_host;
    }

    /// Give the chord a list of address to try to join when it starts.
    /// If Some Address is passed to start() it will be tried before
    /// these addresses.
    pub fn set_join_list(&mut self, list: Vec<A>) {
        self.join_list = list;
    }

    /// Initialize the advert data for this node, pass None to clear it.
    pub fn set_advert(&mut self, data: Option<Vec<u8>>) {
        self.advert = data;
    }

    /// Gets an AssociateChannel connected to this node. The channel will not
    /// return any results until the node is started.
    pub async fn get_associate(&self) -> ChordResult<AssociateChannel<A, I>> {
        let associate_id = self.next_associate_id.fetch_add(1, Ordering::SeqCst);
        let to = self.channel_tx.clone();
        let (to_processor, from) = channel(50);
        let next_associate_id = self.next_associate_id.clone();

        self.channel_tx
            .send((
                ProcessorId::Associate(associate_id),
                Message::Private(PrivateMessage::RegisterAssociate { conn: to_processor }),
            ))
            .await
            .problem(ErrorKind::AssociateClosed)?;

        Ok(AssociateChannel::new(
            associate_id,
            to,
            from,
            next_associate_id,
        ))
    }

    /// Starts the node. This will take ownership of the Chord and return a ChordHandle.
    ///
    /// If passed Some(Address) that address will be prepended to the join list.
    /// If the join list has any elements, the node will try to join each of
    /// them in turn. The first to connect will be used as the address to join.
    /// If no address are reachable, the node will not start.
    /// If the join list is empty, the node will start, implicitly creating a
    /// new chord.
    pub async fn start(mut self, join_addr: Option<A>) -> ChordResult<ChordHandle<A, I, ADAPTOR>> {
        let mut join_list = Vec::new();
        if let Some(addr) = join_addr {
            join_list.insert(0, addr);
        }
        join_list.append(&mut self.join_list);

        // if join_list is not empty, then connect and find our successor
        let mut successor_data: Option<(I, A)> = None;
        if !join_list.is_empty() {
            for addr in join_list.iter() {
                let mut associate = match ADAPTOR::associate_client(addr.clone()) {
                    Ok(associate) => associate,
                    Err(_) => {
                        // Since the associate could not connect here, try the next one.
                        continue;
                    }
                };
                let join_node = associate.successor_of(self.self_id.clone()).await;
                match join_node {
                    Ok(x) => {
                        successor_data = Some(x);
                        break;
                    }
                    // if an invalid response, try next node
                    Err(_) => {}
                }
            }
            // if after the loop, no successor data exists
            // and not hosting as a fallback, quit
            if successor_data.is_none() && !self.join_or_host {
                return Err(ErrorKind::FailedToConnect)?;
            }
        }

        // Start listener task
        let listen_addr = self.listen_addr.clone();
        info!("Listening on {:?}", listen_addr);
        let listener_handle = self
            .adaptor
            .listen_handler(listen_addr, self.channel_tx.clone())
            .problem_wrap(ErrorKind::ListenerHandlerFailed)?;

        // Start maintenance task
        let stabilizer_channel = self.channel_tx.clone();
        let maintenance_handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                let mut errored = false;
                errored |= stabilizer_channel
                    .send((ProcessorId::Internal, PrivateMessage::Stabilize.into()))
                    .await
                    .is_err();
                errored |= stabilizer_channel
                    .send((ProcessorId::Internal, PrivateMessage::FixFingers.into()))
                    .await
                    .is_err();
                errored |= stabilizer_channel
                    .send((ProcessorId::Internal, PrivateMessage::Cleanup.into()))
                    .await
                    .is_err();
                errored |= stabilizer_channel
                    .send((
                        ProcessorId::Internal,
                        PrivateMessage::CheckPredecessor.into(),
                    ))
                    .await
                    .is_err();
                errored |= stabilizer_channel
                    .send((ProcessorId::Internal, PrivateMessage::SaveState.into()))
                    .await
                    .is_err();
                if errored {
                    break;
                }
            }
        });

        let associate_channel = self.get_associate().await?;

        // if there is successor data to set as our successor, connect to that chord
        if let Some((id, addr)) = successor_data {
            let to_successor = self
                .adaptor
                .connect(addr.clone(), Some(id.clone()), self.channel_tx.clone())
                .problem_wrap(ErrorKind::FailedToConnect)?;
            to_successor
                .send(PublicMessage::Introduction {
                    id: self.self_id.clone(),
                    addr: self.self_addr.clone(),
                })
                .await
                .problem(ErrorKind::FailedToConnect)?;
            self.channel_tx
                .send((
                    ProcessorId::Member(id),
                    PrivateMessage::RegisterMember {
                        addr,
                        conn: to_successor,
                    }
                    .into(),
                ))
                .await
                .problem(ErrorKind::ChordStopped)?;
        }

        // Spawn operation task
        let processor_handle = tokio::spawn(async move {
            while let Some((id, operation)) = self.channel_rx.recv().await {
                info!("Processing message from {:?}: {:?}", id, operation);
                match operation {
                    Message::Private(operation) => {
                        let _ = self.process_private(id, operation).await;
                    }
                    Message::Public(operation) => {
                        let _ = self.process_public(id, operation).await;
                    }
                }
            }
            info!("processor thread terminating");
        });

        Ok(ChordHandle {
            listener_handle,
            maintenance_handle,
            processor_handle,

            associate_channel,
        })
    }

    #[instrument]
    async fn send_result(
        &mut self,
        id: ProcessorId<I>,
        result: PublicMessage<A, I>,
    ) -> ChordResult {
        match id {
            ProcessorId::Member(id) => match self.members.get_mut(&id) {
                Some((_, channel)) => {
                    channel.send(result).await?;
                }
                None => {}
            },
            ProcessorId::Associate(id) => match self.associates.get_mut(&id) {
                Some(channel) => {
                    channel.send(result).await?;
                }
                None => {}
            },
            ProcessorId::Internal => {} // Messages sent to the internal channel are ignored.
        }
        Ok(())
    }

    async fn route(
        &mut self,
        to: I,
        from: I,
        channel: ProcessorId<I>,
        exact: bool,
        packet_type: PacketType<A, I>,
    ) -> ChordResult {
        let packet = Packet::new(to, from, channel, exact, packet_type);
        self.route_packet(packet).await
    }

    async fn reply_packet(
        &mut self,
        packet: Packet<A, I>,
        packet_type: PacketType<A, I>,
    ) -> ChordResult {
        self.route_packet(packet.reply_with(&self.self_id, packet_type))
            .await
    }

    async fn route_packet(&mut self, packet: Packet<A, I>) -> ChordResult {
        let mut channel = None;
        // test upper section of btree
        let x = self
            .members
            .range((Excluded(self.self_id.clone()), Unbounded))
            .next();
        if let Some((_, ch)) = x {
            channel = Some(ch);
        } else {
            // test lower section of btree if value not found in upper
            let x = self
                .members
                .range((Unbounded, Excluded(self.self_id.clone())))
                .next();
            if let Some((_, ch)) = x {
                channel = Some(ch);
            }
        }

        match channel {
            Some((_id, channel)) => {
                // println!("routing packet to {:?}", id);
                channel.send(PublicMessage::Route { packet }).await?;
            }
            None => {} // Could not find any connection, data must drop
        }
        Ok(())
    }

    fn successor(&self) -> Option<(&I, &A)> {
        // test upper section of btree
        let x = self
            .members
            .range((Excluded(self.self_id.clone()), Unbounded))
            .next();
        if let Some((id, (addr, _))) = x {
            return Some((id, addr));
        }

        // test lower section of btree
        let x = self
            .members
            .range((Unbounded, Excluded(self.self_id.clone())))
            .next();
        if let Some((id, (addr, _))) = x {
            return Some((id, addr));
        }

        // could not find
        None
    }

    fn in_this_sector(&self, id: &I) -> bool {
        match &self.predecessor {
            // id is in this sector if it is between the predecessor and self_id
            Some(predecessor) => id.is_between(predecessor, &self.self_id),
            // no predecessor means the entire ring is in this sector (at least for the moment)
            None => true,
        }
    }
}

/// A ChordHandle represents a connection to a started Chord.
pub struct ChordHandle<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> {
    listener_handle: Option<JoinHandle<Result<(), <ADAPTOR as ChordAdaptor<A, I>>::Error>>>,
    maintenance_handle: JoinHandle<()>,
    processor_handle: JoinHandle<()>,

    associate_channel: AssociateChannel<A, I>,
}

impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> ChordHandle<A, I, ADAPTOR> {
    /// Get a new AssociateChannel connected to the underlying node.
    pub async fn get_associate(&self) -> ChordResult<AssociateChannel<A, I>> {
        self.associate_channel.duplicate().await
    }

    /// Force the chord to stop
    pub async fn stop(self) {
        if let Some(handle) = &self.listener_handle {
            handle.abort();
        }
        self.maintenance_handle.abort();
        self.processor_handle.abort();

        if let Some(handle) = self.listener_handle {
            let _ = handle.await;
        }
        let _ = self.maintenance_handle.await;
        let _ = self.processor_handle.await;
    }
}
