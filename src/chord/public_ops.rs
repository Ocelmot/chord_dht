use tracing::{info, instrument};

use crate::{
    adaptor::ChordAdaptor,
    chord_id::ChordId,
    error::{ChordResult, ErrorKind, Problem, ProblemWrap},
    Chord, ChordAddress,
};

use super::{
    message::{PacketType, PublicMessage},
    ProcessorId,
};

use std::ops::Bound::{Excluded, Unbounded};

impl<A: ChordAddress, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR> {
    pub(crate) async fn process_public(
        &mut self,
        channel_id: ProcessorId<I>,
        operation: PublicMessage<A, I>,
    ) -> ChordResult {
        match operation {
            // This message is for sending the id to the adaptor initially
            PublicMessage::Introduction { .. } => {}

            // State Operations
            PublicMessage::GetID => {
                self.send_result(
                    channel_id,
                    PublicMessage::ID {
                        id: self.self_id.clone(),
                    },
                )
                .await?;
            }
            PublicMessage::ID { id: _ } => {} // Chord sends this message
            PublicMessage::GetPredecessor {} => {
                let mut pred = None;
                if let Some(pred_id) = &self.predecessor {
                    if let Some((pred_addr, _)) = self.members.get(&pred_id) {
                        pred = Some((pred_id.clone(), pred_addr.clone()));
                    }
                }

                self.send_result(channel_id, PublicMessage::Predecessor { pred })
                    .await?;
            }
            // Step 2 of stabilize procedure: receive predecessor from successor
            PublicMessage::Predecessor { pred } => {
                self.predecessor(channel_id, pred).await?;
            }
            PublicMessage::GetSuccessor {} => {
                let succ = if let Some((id, addr)) = self.successor() {
                    Some((id.clone(), addr.clone()))
                } else {
                    None
                };

                self.send_result(channel_id, PublicMessage::Successor { succ })
                    .await?;
            }
            PublicMessage::Successor { .. } => {}

            // Chord Operations
            PublicMessage::Route { packet } => {
                // if route is to us, handle result
                // otherwise, route the packet along
                if self.in_this_sector(&packet.to) {
                    println!("processing...");
                    // test if packet was supposed to route to an exact node,
                    // but node does not exist
                    if packet.exact && packet.to != self.self_id {
                        self.reply_packet(
                            packet,
                            PacketType::Error {
                                msg: "Node does not exist".to_string(),
                            },
                        )
                        .await?;
                    } else {
                        match packet.packet_type {
                            PacketType::GetSuccessorOf { .. } => {
                                let reply = PacketType::SuccessorOf {
                                    id: self.self_id.clone(),
                                    addr: self.self_addr.clone(),
                                };
                                self.reply_packet(packet, reply).await?;
                            }
                            PacketType::SuccessorOf { addr, id } => {
                                self.send_result(
                                    packet.channel,
                                    PublicMessage::SuccessorOf { addr, id },
                                )
                                .await?;
                            }
                            PacketType::Dialback { id, addr } => {
                                if !self.members.contains_key(&id) {
                                    // there is no existing connection to this node, create one
                                    let new_connection = self
                                        .adaptor
                                        .connect(
                                            addr.clone(),
                                            Some(self.self_id.clone()),
                                            self.channel_tx.clone(),
                                        )
                                        .problem_wrap(ErrorKind::AssociateConnectionFailed)?;
                                    new_connection
                                        .send(PublicMessage::Introduction {
                                            id: self.self_id.clone(),
                                            addr: self.self_addr.clone(),
                                        })
                                        .await
                                        .problem(ErrorKind::SendError)?;
                                    self.members.insert(id, (addr, new_connection));
                                }
                            }
                            PacketType::GetAdvert => {
                                let reply = PacketType::Advert {
                                    id: self.self_id.clone(),
                                    data: self.advert.clone(),
                                };
                                self.reply_packet(packet, reply).await?;
                            }
                            PacketType::Advert { id, data } => {
                                self.send_result(
                                    packet.channel,
                                    PublicMessage::AdvertOf { id, data },
                                )
                                .await?;
                            }
                            PacketType::Error { msg } => {
                                self.send_result(packet.channel, PublicMessage::Error { msg })
                                    .await?;
                            }
                        }
                    }
                } else {
                    println!("routing...");
                    self.route_packet(packet).await?;
                }
            }
            PublicMessage::GetSuccessorOf { id } => {
                self.get_successor_of(id, channel_id).await?;
            }
            PublicMessage::SuccessorOf { addr: _, id: _ } => {} // Chord sends this message

            // Step 3 of stabilize procedure: respond to incoming notifications from predecessor
            PublicMessage::Notify => {
                self.notify(channel_id).await;
            }

            // Other
            PublicMessage::GetAdvertOf { id } => {
                self.get_advert(id, channel_id).await?;
            }
            PublicMessage::AdvertOf { id: _, data: _ } => {}
            PublicMessage::GetPeerAddresses => {
                self.get_peer_addrs(channel_id).await?;
            }
            PublicMessage::PeerAddresses { .. } => {} // Chord will not request for this

            // Debug
            PublicMessage::Error { msg: _ } => {} // Chord sends this message
            PublicMessage::Debug { msg: _ } => {
                self.debug(channel_id).await?;
            }
        };
        Ok(())
    }

    // Step 2 of stabilize procedure: receive predecessor from successor
    #[instrument]
    async fn predecessor(
        &mut self,
        channel_id: ProcessorId<I>,
        pred: Option<(I, A)>,
    ) -> ChordResult {
        if let ProcessorId::Member(channel_id) = channel_id {
            // if message was from a member
            if let Some((successor, _)) = self.successor() {
                // if there is a successor
                let successor = successor.clone();
                if successor == channel_id {
                    // if the message was from our successor
                    if let Some((pred_id, pred_addr)) = pred {
                        // if the predecessor exists
                        if pred_id.is_between(&self.self_id, &successor) && successor != pred_id {
                            // open connection to new successor
                            let new_conn = self
                                .adaptor
                                .connect(
                                    pred_addr.clone(),
                                    Some(pred_id.clone()),
                                    self.channel_tx.clone(),
                                )
                                .problem_wrap(ErrorKind::AssociateConnectionFailed)?;
                            new_conn
                                .send(PublicMessage::Introduction {
                                    id: self.self_id.clone(),
                                    addr: self.self_addr.clone(),
                                })
                                .await
                                .problem(ErrorKind::SendError)?;
                            self.members.insert(pred_id, (pred_addr, new_conn));
                        }
                    }
                }
            }
            if let Some((successor, _)) = self.successor() {
                // successor may have updated
                self.send_result(
                    ProcessorId::Member(successor.clone()),
                    PublicMessage::Notify,
                )
                .await?;
            }
        }
        Ok(())
    }

    // Step 3 of stabilize procedure: respond to incoming notifications from predecessor
    #[instrument]
    async fn get_successor_of(&mut self, id: I, channel_id: ProcessorId<I>) -> ChordResult {
        info!("getting successor");
        // if id is ours, return answer, otherwise query the chord
        if self.in_this_sector(&id) {
            info!("In sector");
            let addr = self.self_addr.clone();
            let id = self.self_id.clone();
            self.send_result(channel_id, PublicMessage::SuccessorOf { addr, id })
                .await?;
        } else {
            info!("routing");
            self.route(
                id.clone(),
                self.self_id.clone(),
                channel_id,
                false,
                PacketType::GetSuccessorOf { id },
            )
            .await?;
        }
        Ok(())
    }

    async fn notify(&mut self, channel_id: ProcessorId<I>) {
        if let ProcessorId::Member(new_pred) = channel_id {
            if let Some(predecessor) = &self.predecessor {
                if predecessor.is_between(&new_pred, &self.self_id) {
                    return; // predecessor exists and is between new predecessor and self
                }
            }
            // predecessor needs updating (does not exist or not between new and self)
            self.predecessor = Some(new_pred);
        }
    }

    async fn get_advert(&mut self, id: I, channel_id: ProcessorId<I>) -> ChordResult {
        if self.self_id == id {
            self.send_result(
                channel_id,
                PublicMessage::AdvertOf {
                    id: self.self_id.clone(),
                    data: self.advert.clone(),
                },
            )
            .await?;
        } else {
            self.route(
                id,
                self.self_id.clone(),
                channel_id,
                true,
                PacketType::GetAdvert,
            )
            .await?;
        }
        Ok(())
    }

    async fn get_peer_addrs(&mut self, channel_id: ProcessorId<I>) -> ChordResult {
        let mut addrs = Vec::with_capacity(self.members.len());
        for (_id, (addr, _sender)) in &self.members {
            addrs.push(addr.clone());
        }

        let msg = PublicMessage::PeerAddresses { addrs };
        self.send_result(channel_id, msg).await?;
        Ok(())
    }

    async fn debug(&mut self, channel_id: ProcessorId<I>) -> ChordResult {
        let mut msg = String::new();
        // show predecessor/id/successor
        msg = msg
            + &format!(
                "Id: {:?} Predecessor: {:?} Successor: {:?}\n",
                self.self_id,
                self.predecessor,
                self.successor()
            );
        // show nodes
        if self.members.is_empty() {
            msg = msg + "No members connected.\n"
        } else {
            msg = msg + "Connected nodes:\n";
            // print upper section first
            for (id, (addr, _)) in self
                .members
                .range((Excluded(self.self_id.clone()), Unbounded))
            {
                msg = msg + &format!("member id: {:?}, at addr: {:?}\n", id, addr);
            }

            // then print lower section
            for (id, (addr, _)) in self
                .members
                .range((Unbounded, Excluded(self.self_id.clone())))
            {
                msg = msg + &format!("member id: {:?}, at addr: {:?}\n", id, addr);
            }
        }

        self.send_result(channel_id, PublicMessage::Debug { msg })
            .await
    }
}
