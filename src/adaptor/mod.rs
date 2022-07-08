use crate::{
	chord_id::ChordId,
	associate::{AssociateRequest, AssociateResponse},
	ChordAddress, chord::{message::{Message, PublicMessage}, ProcessorId}
};

use std::{fmt::Debug, sync::{atomic::AtomicU32, Arc}};

use tokio::{task::JoinHandle, sync::mpsc::{Sender, Receiver}};

/// An implementation of ChordAdaptor for addresses that implement ToSocketAddrs
pub mod tcp_adaptor;

/// A ChordAdaptor is instantiated within a chord node to allow the node listen
/// for incoming connections and to convert addresses to outgoing connections.
/// 
/// Implementing this trait allows the chord logic to be implemented for other
/// protocols or types of connections.
pub trait ChordAdaptor<A: ChordAddress, I: ChordId>: Send + Sync + 'static + Debug{

	/// Create a new ChordAdaptor instance
	fn new(id: I, addr: A, next_associate_id: Arc<AtomicU32>) -> Self;

	/// Spawn a task to listen for new connections to listen_addr.
	/// New channels are then registered through the provided sender channel.
	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, Message<A, I>)>) -> JoinHandle<()>;

	/// Return the Sender portion of a channel that connects to a node at addr.
	/// If an id is provided, the connection is a member connection, otherwise
	/// it is an associate connection.
	/// Incoming messages should be routed to the provided Sender.
	fn connect(&self, addr: A, id: Option<I>, channel_from_connection: Sender<(ProcessorId<I>, Message<A, I>)>) -> Sender<PublicMessage<A, I>>;

	/// Create a connection to the node at addr as an associate.
	/// Return an AssociateClient representing that connection.
	fn associate_client(addr: A) -> AssociateClient<A, I>;
}

/// An AssociateClient behaves very similarly to an AssociateChannel,
/// the distinction being that it is connected to a remote node at a
/// particular address rather than a node spawned on the current
/// machine.
pub struct AssociateClient<A: ChordAddress, I: ChordId>{
	to: Sender<PublicMessage<A, I>>,
	from: Receiver<PublicMessage<A, I>>,
}

impl<A: ChordAddress, I: ChordId> AssociateClient<A, I> {
	/// Create an AssociateClient from two streams, a Sender and a Receiver.
	pub fn new(to: Sender<PublicMessage<A, I>>, from: Receiver<PublicMessage<A, I>>) -> Self{
		AssociateClient {
			to,
			from
		}
	}

	/// Send an AssociateRequest directly.
	/// 
	/// The chord's response can be recieved later via the recv_op() method.
	/// If multiple requests are made, their responses may arrive in any
	/// order.
	pub async fn send_op(&self, msg: AssociateRequest<A, I>) {
		self.to.send(msg.into()).await;
	}

	/// Receive an AssociateResponse directly.
	/// 
	/// Receive a response from the chord.
	/// If multiple requests have been sent, the response received may be a
	/// response to any outstanding request.
	pub async fn recv_op(&mut self) -> Option<AssociateResponse<A, I>>{
		loop{
			let msg = self.from.recv().await;
			match msg {
				Some(msg) => {
					if let Some(msg) = msg.into() {
						return Some(msg);
					}
				},
				None => {return None},
			}
		}
	}

	/// Query for the successor of a particular node,
	/// and await the response as a single operation.
	/// 
	/// If other responses arrive, they are discarded.
	pub async fn successor_of(&mut self, id: I) -> Option<(I, A)>{
		self.send_op(AssociateRequest::GetSuccessorOf{id}).await;
		loop{
			let response = self.recv_op().await;
			match response{
				Some(AssociateResponse::SuccessorOf { id, addr }) => {
					return Some((id, addr));
				},
				_ => return None,
			}
		}
	}


}