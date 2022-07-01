use std::{time::Duration, collections::BTreeMap, ops::Bound::{Excluded, Unbounded}};

mod internal_ops;
mod stabilize;


use crate::{ chord_id::ChordId, adaptor::ChordAdaptor};


use tokio::sync::mpsc::{channel, Sender, Receiver, error::SendError};
use tokio::task::JoinHandle;
use tokio::time::interval;


#[derive(Debug)]
pub enum ChordOperation<A, I: ChordId>{
	// State Operations
	GetID,
	GetPredecessor,

	// Internal Operations
	RegisterConnection{conn: Sender<ChordOperationResult<A, I>>},

	// Chord Operations
	Route{packet: Packet<A, I>},
	LookupNode{id: I},

	// todo

	Ping(I),

	Stabilize,

	UpdateSuccessor{node: I, pred: I},
	Notify,
	Notified(I),
	FixFingers,
	CheckPredecessor,
	Cleanup,
}

#[derive(Debug)]
pub struct Packet<A, I: ChordId>{
	to: I,
	from: I,
	channel: ProcessorId<I>,
	exact: bool,
	checksum: u32,
	packet_type: PacketType<A, I>
}

#[derive(Debug)]
pub enum PacketType<A, I: ChordId>{
	Lookup{id: I},
	LookupReply{addr: A},
	Error{msg: String}
}

#[derive(Debug)]
pub enum ChordOperationResult<A, I: ChordId>{
	ID(I),
	Predecessor(Option<I>),
	LookupNode(A),
	Route(Packet<A, I>),
	Error(String),
}

#[derive(Debug, Clone)]
pub enum ProcessorId<M>{
	Member(M),
	Associate(u32),
}

pub(crate) struct ChordProcessor<A, I: ChordId, ADAPTOR: ChordAdaptor<A, I>>{
	// Core data
	self_addr: A,
	self_id: I,
	predecessor: Option<I>,
	finger_table: Vec<I>,

	// Connections
	adaptor: ADAPTOR,
	members: BTreeMap<I, Sender<ChordOperationResult<A, I>>>,
	associates: BTreeMap<u32, Sender<ChordOperationResult<A, I>>>,
	
	// Operations channel
	channel_rx: Receiver<(ProcessorId<I>, ChordOperation<A, I>)>,
	channel_tx: Sender<(ProcessorId<I>, ChordOperation<A, I>)>,

	// Task Handles
	listener_handle: Option<JoinHandle<()>>,
	stabilizer_handle: Option<JoinHandle<Result<(), SendError<ChordOperation<A, I>>>>>,
}

impl<A: Clone + Send + 'static, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> ChordProcessor<A, I, ADAPTOR>{

	pub fn new (self_addr: A, self_id: I) -> Self{
		let (channel_tx, channel_rx) = channel(50);
		let adaptor = ADAPTOR::new();
		let connections = BTreeMap::new();

		ChordProcessor{
			// Core data
			self_addr,
			self_id,
			predecessor: None,
			finger_table: Vec::new(),

			// Connections
			adaptor,
			members: connections,
			associates: BTreeMap::new(),

			// Operations channel
			channel_rx,
			channel_tx,

			// Task Handles
			listener_handle: None,
			stabilizer_handle: None,
		}
	}

	pub fn get_channel(&self) -> Sender<(ProcessorId<I>, ChordOperation<A, I>)>{
		self.channel_tx.clone()
	}

	pub async fn start(mut self, join_addr: Option<A>) -> JoinHandle<()>{
		// if join_addr is not None, then connect and find our successor
		//let successor = None;
		if let Some(join_addr) = join_addr {
			// this nonsense should be rewritten at some point.
			// let (from_tx, from_rx) = channel(50);
			// let to_tx = self.adaptor.connect(join_addr, None, from_tx);
			// to_tx.send(ChordOperationResult::LookupNode(self.self_id.clone()));
			
		}

		// Start listener task
		let listener_handle = self.adaptor.listen_handler(self.self_addr.clone(), self.channel_tx.clone());
		self.listener_handle = Some(listener_handle);

		// Start maintenance task
		let stabilizer_channel = self.channel_tx.clone();
		let stabilizer_handle = tokio::spawn(async move{
			let mut interval = interval(Duration::from_secs(15));
			loop{
				interval.tick().await;
				// stabilizer_channel.send((ProcessorId::Associate(0), ChordOperation::Stabilize)).await;
				// stabilizer_channel.send(ChordOperation::Notify).await?;
				// stabilizer_channel.send(ChordOperation::FixFingers).await?;
				// stabilizer_channel.send(ChordOperation::CheckPredecessor).await?;
			}
			#[allow(unreachable_code)]
			Ok::<(), SendError<ChordOperation<A, I>>>(())
		});
		self.stabilizer_handle = Some(stabilizer_handle);

		// Spawn operation task
		tokio::spawn(async move{
			while let Some((id, operation)) = self.channel_rx.recv().await{
				self.process_operation(id, operation).await;
			}
		})
	}

	async fn process_operation(&mut self, channel_id: ProcessorId<I>, operation: ChordOperation<A, I>){
		match operation{
			// State Operations
			ChordOperation::GetID => {
				self.send_result(channel_id, ChordOperationResult::ID(self.self_id.clone())).await;
			},
			ChordOperation::GetPredecessor { } => {
				self.send_result(channel_id, ChordOperationResult::Predecessor(self.predecessor.clone())).await;
			},

			// Internal Operations
			ChordOperation::RegisterConnection { conn }=>{
				self.register_connection(channel_id, conn).await;
			},
			// Chord Operations
			ChordOperation::Route{packet} => {
				// if route is to us, handle result
				// otherwise, route the packet along
				if self.in_this_sector(&packet.to) {
					match packet.packet_type {
						PacketType::Lookup { .. } => {
							let reply = PacketType::LookupReply { addr: self.self_addr.clone() };
							self.reply_packet(packet, reply).await;
						},
						PacketType::LookupReply { addr } => {
							self.send_result(packet.channel, ChordOperationResult::LookupNode(addr)).await;
						},
						PacketType::Error { msg } => {
							self.send_result(packet.channel, ChordOperationResult::Error(msg)).await;
						},
					}
				} else {
					self.route_packet(packet).await;
				}
			},

			ChordOperation::LookupNode { id } => {
				// if id is ours, return answer, otherwise query the chord
				if self.in_this_sector(&id) {
					let addr = self.self_addr.clone();
					self.send_result(channel_id, ChordOperationResult::LookupNode(addr)).await;
				} else {
					self.route(id.clone(), self.self_id.clone(), channel_id, false, PacketType::Lookup { id }).await;
				}
			}



			/////// Conversion point


			ChordOperation::Ping(_) => {}, //self.ping().await,
			//

			ChordOperation::Stabilize => {
				self.stabilize().await;
			},
			ChordOperation::Notify => {
				self.notify().await;
			},
			ChordOperation::Notified(notified_id) => {
				self.notified(notified_id).await;               
			},
			ChordOperation::FixFingers => {
				self.fix_fingers().await;
			},
			ChordOperation::CheckPredecessor => {
				self.check_predecessor().await;
			},
			ChordOperation::Cleanup => {
				// self.cleanup().await;
			},
			ChordOperation::UpdateSuccessor { node, pred } => {
				self.update_successor(node, pred).await;
			},
			
		};

	}



	async fn send_result(&mut self, id: ProcessorId<I>, result: ChordOperationResult<A, I>){
		match id{
			ProcessorId::Member(id) => {
				match self.members.get_mut(&id){
					Some(channel) => {
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
		}
	}

	async fn route(&mut self, to: I, from: I, channel: ProcessorId<I>, exact: bool, packet_type: PacketType<A, I>) {
		let packet = Packet{
			to,
			from,
			channel,
			exact,
			checksum: 78,
			packet_type,
		};
		self.route_packet(packet).await;
	}

	async fn reply_packet(&mut self, packet: Packet<A, I>, packet_type: PacketType<A, I>){
		let pack = Packet{
			to: packet.from,
			from: self.self_id.clone(),
			channel: packet.channel,
			exact: packet.exact,
			checksum: packet.checksum,
			packet_type,
		};
		self.route_packet(pack).await;
	}

	async fn route_packet(&mut self, packet: Packet<A, I>){
		let mut channel = None;
		// test upper section of btree
		let x = self.members.range((Excluded(self.self_id.clone()), Unbounded)).next();
		if let Some((_, ch)) = x {
			channel = Some(ch);
		}

		// test lower section of btree
		let x = self.members.range((Unbounded, Excluded(self.self_id.clone()))).next();
		if let Some((_, ch)) = x {
			channel = Some(ch);
		}

		match channel {
			Some(channel) => {
				channel.send(ChordOperationResult::Route(packet)).await;
			},
			None => {}, // Could not find any connection, data must drop
		}
	}

	fn successor(&self) -> Option<&I> {
		// test upper section of btree
		let x = self.members.range((Excluded(self.self_id.clone()), Unbounded)).next();
		if let Some((id, _)) = x {
			return Some(id);
		}

		// test lower section of btree
		let x = self.members.range((Unbounded, Excluded(self.self_id.clone()))).next();
		if let Some((id, _)) = x {
			return Some(id);
		}

		// could not find
		None
	}

	fn in_this_sector(&self, id: &I) -> bool {
		match self.successor() {
			// id is in this sector if it is between self_id and the successor id
			Some(successor) => {
				id.is_between(&self.self_id, successor)
			},
			// no successor means the entire ring is in this sector
			None => true,
		}
	}

}