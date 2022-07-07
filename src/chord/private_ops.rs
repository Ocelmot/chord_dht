use tokio::sync::mpsc::Sender;
use tracing::{instrument, info};

use crate::{Chord, chord_id::ChordId, adaptor::ChordAdaptor, ChordAddress};

use super::{ProcessorId, message::{PrivateMessage, PublicMessage, Packet, PacketType}};




impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR>{

	pub(crate) async fn process_private(&mut self, channel_id: ProcessorId<I>, operation: PrivateMessage<A, I>){
		match operation{
			PrivateMessage::RegisterMember {addr, conn } => {
				self.register_member(channel_id, addr, conn).await;
			},
			PrivateMessage::RegisterAssociate { conn } => {
				self.register_associate(channel_id, conn).await;
			},
			PrivateMessage::Stabilize => {
				self.stabilize(channel_id).await;
			},
			PrivateMessage::FixFingers => {
				self.fix_fingers(channel_id).await;
			},
			PrivateMessage::CheckPredecessor => {
				self.check_predecessor(channel_id).await;
			},
			PrivateMessage::Cleanup =>{
				self.cleanup(channel_id).await;
			}
		}
	}


	

	#[instrument]
	async fn register_member(&mut self, channel_id: ProcessorId<I>, addr: A, conn: Sender<PublicMessage<A, I>>){
		info!("Entered register_member!");
		match channel_id {
			ProcessorId::Member(channel_id) => {
				self.members.insert(channel_id, (addr, conn));
			},
			ProcessorId::Associate(_) => {},
			ProcessorId::Internal => {}, // No way to register a connection as the internal channel
		};
	} 
	
	#[instrument]
	async fn register_associate(&mut self, channel_id: ProcessorId<I>, conn: Sender<PublicMessage<A, I>>){
		match channel_id {
			ProcessorId::Member(_) => {},
			ProcessorId::Associate(channel_id) => {
				self.associates.insert(channel_id, conn);
			},
			ProcessorId::Internal => {}, // No way to register a connection as the internal channel
		};
	} 

	/// Starts the stabilize procedure.
	/// Sequence: stabilize -> get_predecessor -> predecessor -> notify
	async fn stabilize(&mut self, channel_id: ProcessorId<I>){
		// make sure that message came from internal channel
		if let ProcessorId::Internal = channel_id{ 
			if let Some((successor, _)) = self.successor(){
				let id = ProcessorId::Member(successor.clone());
				self.send_result(id, PublicMessage::GetPredecessor).await;
			}
		}
	}

	// Trigger this node to try to fix one of its fingers
	async fn fix_fingers(&mut self, channel_id: ProcessorId<I>){
		// make sure that message came from internal channel
		if let ProcessorId::Internal = channel_id { 
			let next_index = I::next_index(self.finger_index);
			self.finger_index = next_index;
			let finger_id = self.self_id.calculate_finger(next_index);
			let packet_type = PacketType::Dialback{id: self.self_id.clone(), addr: self.self_addr.clone()};
			let packet = Packet::new(finger_id.clone(), self.self_id.clone(), ProcessorId::Associate(0), false, packet_type);
			self.route_packet(packet).await;
		}
	}

	async fn check_predecessor(&mut self, channel_id: ProcessorId<I>){
		if let ProcessorId::Internal = channel_id{ // make sure that message came from internal channel
			if let Some(predecessor) = self.predecessor.clone(){      // this node thinks it has a predecessor,
				if !self.members.contains_key(&predecessor) {            // but cannot find its connection as a member, 
					self.predecessor = None;                             // therefore clear the predecessor
				}
			}
		}
	}

	async fn cleanup(&mut self, channel_id: ProcessorId<I>){
		// make sure that message came from internal channel
		if let ProcessorId::Internal = channel_id{ 
			// cleanup members
			self.members.retain(|_, (_, ch)| {
				!ch.is_closed()
			});
			
			// cleanup associates
			self.associates.retain(|_, ch| {
				!ch.is_closed()
			});

		}
	}

}