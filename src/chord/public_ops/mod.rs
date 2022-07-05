use tracing::{instrument, info};

use crate::{chord_id::ChordId, adaptor::ChordAdaptor, Chord, ChordAddress};

use super::{ProcessorId, message::{PublicMessage, PacketType}};

use std::ops::Bound::{Excluded, Unbounded};






impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR>{

    pub(crate) async fn process_public(&mut self, channel_id: ProcessorId<I>, operation: PublicMessage<A, I>){
		match operation{
			// State Operations
			PublicMessage::GetID => {
				self.send_result(channel_id, PublicMessage::ID{id: self.self_id.clone()}).await;
			},
			PublicMessage::GetPredecessor { } => {
                let mut pred = None;
                if let Some(pred_id) = &self.predecessor{
                    if let Some((pred_addr, _)) = self.members.get(&pred_id){
                        pred = Some((pred_id.clone(), pred_addr.clone()));
                    }
                }

				self.send_result(channel_id, PublicMessage::Predecessor{pred}).await;
			},

			// Chord Operations
			PublicMessage::Route{packet} => {
				// if route is to us, handle result
				// otherwise, route the packet along
				if self.in_this_sector(&packet.to) {
					match packet.packet_type {
						PacketType::GetSuccessorOf { .. } => {
							let reply = PacketType::SuccessorOf { id: self.self_id.clone(), addr: self.self_addr.clone() };
							self.reply_packet(packet, reply).await;
						},
						PacketType::SuccessorOf { addr, id } => {
							self.send_result(packet.channel, PublicMessage::SuccessorOf{addr, id}).await;
						},
						PacketType::Error { msg } => {
							self.send_result(packet.channel, PublicMessage::Error{msg}).await;
						},
					}
				} else {
					self.route_packet(packet).await;
				}
			},

			PublicMessage::GetSuccessorOf { id } => {
				self.get_successor_of(id, channel_id).await;
			}


			PublicMessage::ID { id } => todo!(),
            // Step 2 of stabilize procedure: receive predecessor from successor
			PublicMessage::Predecessor { pred} => {
				self.predecessor(channel_id, pred).await;
            },
            // Step 3 of stabilize procedure: respond to incoming notifications from predecessor
            PublicMessage::Notify => {
                self.notify(channel_id).await;
            }
			PublicMessage::SuccessorOf { addr, id } => todo!(),
			PublicMessage::Error { msg } => todo!(),


            PublicMessage::Introduction { .. } => unimplemented!("This message is for sending the id to the adaptor initially"),
			
			PublicMessage::Debug { msg } => {
				self.debug(channel_id).await;
			},
		};

	}

	// Step 2 of stabilize procedure: receive predecessor from successor
	#[instrument]
    async fn predecessor(&mut self, channel_id: ProcessorId<I>, pred: Option<(I, A)>) {
        if let ProcessorId::Member(channel_id) = channel_id { // if message was from a member
			if let Some(successor) = self.successor(){ // if there is a successor
				let successor = successor.clone();
				if successor == channel_id { // if the message was from our successor
					if let Some((pred_id, pred_addr)) = pred { // if the predecessor exists
						if pred_id.is_between(&self.self_id, &successor) && successor != pred_id{
							// open connection to new successor
							let new_conn = self.adaptor.connect(pred_addr.clone(), Some(pred_id.clone()), self.channel_tx.clone());
							new_conn.send(PublicMessage::Introduction { id: self.self_id.clone(), addr: self.self_addr.clone() }).await;
							self.members.insert(pred_id, (pred_addr, new_conn));
						}
					}
				}
			}
			if let Some(successor) = self.successor(){ // successor may have updated
				self.send_result(ProcessorId::Member(successor.clone()), PublicMessage::Notify).await;
			}
		}
    }

	// Step 3 of stabilize procedure: respond to incoming notifications from predecessor
	#[instrument]
    async fn get_successor_of(&mut self, id: I, channel_id: ProcessorId<I>) {
		info!("getting successor");
        // if id is ours, return answer, otherwise query the chord
        if self.in_this_sector(&id) {
			info!("In sector");
			let addr = self.self_addr.clone();
			let id = self.self_id.clone();
			self.send_result(channel_id, PublicMessage::SuccessorOf{addr, id}).await;
		} else {
			info!("routing");
			self.route(id.clone(), self.self_id.clone(), channel_id, false, PacketType::GetSuccessorOf { id }).await;
		}
    }

    async fn notify(&mut self, channel_id: ProcessorId<I>){
        if let ProcessorId::Member(new_pred) = channel_id { 
            if let Some(predecessor) = &self.predecessor {
                if predecessor.is_between(&new_pred, &self.self_id){
                    return; // predecessor exists and is between new predecessor and self
                }
            }
            // predecessor needs updating (does not exist or not between new and self)
            self.predecessor = Some(new_pred);
        }
    }




	async fn debug(&mut self, channel_id: ProcessorId<I>){
		let mut msg = String::new();

		if self.members.is_empty() {
			msg = msg + "No members connected."
		}else{
			// print upper section first
			for (id, (addr, _)) in self.members.range((Excluded(self.self_id.clone()), Unbounded)) {
				msg = msg + &format!("member id: {:?}, at addr: {:?}\n", id, addr);
			}

			// then print lower section
			for (id, (addr, _)) in self.members.range((Unbounded, Excluded(self.self_id.clone()))) {
				msg = msg + &format!("member id: {:?}, at addr: {:?}\n", id, addr);
			}
		}




		self.send_result(channel_id, PublicMessage::Debug { msg }).await;
	}
    
}


