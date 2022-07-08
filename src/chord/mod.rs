use crate::{adaptor::ChordAdaptor, associate::AssociateChannel, chord_id::ChordId, ChordAddress};

use std::{time::Duration, collections::BTreeMap, ops::Bound::{Excluded, Unbounded}, sync::{atomic::{AtomicU32, Ordering}, Arc}, fmt::Debug};

use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::task::JoinHandle;
use tokio::time::interval;

use serde::{Serialize, Deserialize};


pub(crate) mod message;

use message::Message;
use tracing::{info, instrument};

mod private_ops;
mod public_ops;

use self::message::{PacketType, Packet, PrivateMessage, PublicMessage};

/// An internal id associated with channels to determine the relationship of
/// that channel to the node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessorId<M>{
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
pub struct Chord<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>>{
	// Core data
	self_addr: A,
	self_id: I,
	predecessor: Option<I>,

	// Connections
	adaptor: ADAPTOR,
	members: BTreeMap<I, (A, Sender<PublicMessage<A, I>>)>,
	associates: BTreeMap<u32, Sender<PublicMessage<A, I>>>,
	finger_index: u32,
	next_associate_id: Arc<AtomicU32>,

	// Operations channel
	channel_rx: Receiver<(ProcessorId<I>, Message<A, I>)>,
	channel_tx: Sender<(ProcessorId<I>, Message<A, I>)>,
}

impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR>{

	/// Creates a new Chord instance with the provided id and address.
	/// The address will be passed to the adaptor to function as the
	/// listen address.
	pub fn new (self_addr: A, self_id: I) -> Self{
		let (channel_tx, channel_rx) = channel(50);
		let next_associate_id = Arc::new(AtomicU32::new(1));
		let adaptor = ADAPTOR::new(self_id.clone(), self_addr.clone(), next_associate_id.clone());
		let connections = BTreeMap::new();

		Chord{
			// Core data
			self_addr,
			self_id,
			predecessor: None,

			// Connections
			adaptor,
			members: connections,
			associates: BTreeMap::new(),
			finger_index: 1,
			next_associate_id,

			// Operations channel
			channel_rx,
			channel_tx,
		}
	}

	/// Gets an AssociateChannel connected to this node. The channel will not
	/// return any results until the node is started.
	pub async fn get_associate(&self) -> AssociateChannel<A, I>{

		let associate_id = self.next_associate_id.fetch_add(1, Ordering::SeqCst);
		let to = self.channel_tx.clone();
		let (to_processor, from) = channel(50);
		let next_associate_id = self.next_associate_id.clone();

		self.channel_tx.send((ProcessorId::Associate(associate_id), Message::Private(PrivateMessage::RegisterAssociate{ conn: to_processor }))).await;

		AssociateChannel::new(associate_id, to, from, next_associate_id)
	}

	/// Starts the node. This will take ownership of the Chord and return a ChordHandle.
	/// If passed Some(Address) the node will try to join that chord. Otherwise
	/// it will be a new chord ready to be joined.
	pub async fn start(mut self, join_addr: Option<A>) -> ChordHandle<A, I> {
		// panic!("listening on: {:?}, joining to: {:?}", self.self_addr, join_addr);
		// if join_addr is not None, then connect and find our successor
		let mut successor_data: Option<(I, A)> = None;
		if let Some(join_addr) = join_addr {
			let mut associate = ADAPTOR::associate_client(join_addr);
			let join_node = associate.successor_of(self.self_id.clone()).await;
			match join_node{
				Some(x) => successor_data = Some(x),
				None => todo!("Failed to find a successor in chord"),
			}
		}

		// Start listener task
		let listener_handle = self.adaptor.listen_handler(self.self_addr.clone(), self.channel_tx.clone());

		// Start maintenance task
		let stabilizer_channel = self.channel_tx.clone();
		let maintenance_handle = tokio::spawn(async move{
			let mut interval = interval(Duration::from_secs(15));
			loop{
				interval.tick().await;
				stabilizer_channel.send((ProcessorId::Internal, PrivateMessage::Stabilize.into())).await;
				stabilizer_channel.send((ProcessorId::Internal, PrivateMessage::FixFingers.into())).await;
				stabilizer_channel.send((ProcessorId::Internal, PrivateMessage::Cleanup.into())).await;
				stabilizer_channel.send((ProcessorId::Internal, PrivateMessage::CheckPredecessor.into())).await;
			}
			// #[allow(unreachable_code)]
			// Ok::<(), SendError<Message<A, I>>>(())
		});
		

		let associate_channel = self.get_associate().await;

		// if there is successor data to set as our successor, connect to that chord
		if let Some((id, addr)) = successor_data {
			let to_successor = self.adaptor.connect(addr.clone(), Some(id.clone()), self.channel_tx.clone());
			to_successor.send(PublicMessage::Introduction { id: self.self_id.clone(), addr: self.self_addr.clone() }).await;
			self.channel_tx.send((ProcessorId::Member(id), PrivateMessage::RegisterMember{addr, conn: to_successor }.into())).await;
		}

		// Spawn operation task
		let processor_handle = tokio::spawn(async move{
			while let Some((id, operation)) = self.channel_rx.recv().await{
				info!("Processing message from {:?}: {:?}", id, operation);
				match operation{
					Message::Private(operation) => {
						self.process_private(id, operation).await;
					},
					Message::Public(operation) => {
						self.process_public(id, operation).await;
					},
				}
			}
			info!("processor thread terminating");
		});



		ChordHandle{
			listener_handle,
			maintenance_handle,
			processor_handle,

			associate_channel,
		}
	}


	


	#[instrument]
	async fn send_result(&mut self, id: ProcessorId<I>, result: PublicMessage<A, I>){
		// info!("Sending result");
		match id{
			ProcessorId::Member(id) => {
				match self.members.get_mut(&id){
					Some((_, channel)) => {
						channel.send(result).await;
					},
					None => {},
				}
			},
			ProcessorId::Associate(id) => {
				match self.associates.get_mut(&id){
					Some(channel) => {
						channel.send(result).await;
					},
					None => {},
				}
			},
			ProcessorId::Internal => {}, // Messages sent to the internal channel are ignored.
		}
	}

	async fn route(&mut self, to: I, from: I, channel: ProcessorId<I>, exact: bool, packet_type: PacketType<A, I>) {
		let packet = Packet::new(
			to,
			from,
			channel,
			exact,
			packet_type,
		);
		self.route_packet(packet).await;
	}

	async fn reply_packet(&mut self, packet: Packet<A, I>, packet_type: PacketType<A, I>){
		self.route_packet(packet.reply_with(&self.self_id, packet_type)).await;
	}

	async fn route_packet(&mut self, packet: Packet<A, I>){
		let mut channel = None;
		// test upper section of btree
		let x = self.members.range((Excluded(self.self_id.clone()), Unbounded)).next();
		if let Some((_, ch)) = x {
			channel = Some(ch);
		}else{
			// test lower section of btree if value not found in upper
			let x = self.members.range((Unbounded, Excluded(self.self_id.clone()))).next();
			if let Some((_, ch)) = x {
				channel = Some(ch);
			}
		}


		match channel {
			Some((id, channel)) => {
				// println!("routing packet to {:?}", id);
				channel.send(PublicMessage::Route{packet}).await;
			},
			None => {}, // Could not find any connection, data must drop
		}
	}

	fn successor(&self) -> Option<(&I, &A)> {
		// test upper section of btree
		let x = self.members.range((Excluded(self.self_id.clone()), Unbounded)).next();
		if let Some((id, (addr, _))) = x {
			return Some((id, addr));
		}

		// test lower section of btree
		let x = self.members.range((Unbounded, Excluded(self.self_id.clone()))).next();
		if let Some((id, (addr, _))) = x {
			return Some((id, addr));
		}

		// could not find
		None
	}

	fn in_this_sector(&self, id: &I) -> bool {
		match &self.predecessor {
			// id is in this sector if it is between the predecessor and self_id
			Some(predecessor) => {
				id.is_between(predecessor, &self.self_id)
			},
			// no predecessor means the entire ring is in this sector (at least for the moment)
			None => true,
		}
	}

}



/// A ChordHandle represents a connection to a started Chord.
pub struct ChordHandle<A: ChordAddress, I: ChordId>{
	listener_handle: JoinHandle<()>,
	maintenance_handle: JoinHandle<()>,
	processor_handle: JoinHandle<()>,

	associate_channel: AssociateChannel<A, I>,
}

impl<A: ChordAddress, I: ChordId> ChordHandle<A, I> {
	/// Get a new AssociateChannel connected to the underlying node.
	pub async fn get_associate(&self) -> AssociateChannel<A, I>{
		self.associate_channel.duplicate().await
	}
}