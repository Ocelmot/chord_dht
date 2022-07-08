use std::{sync::{atomic::{AtomicU32, Ordering}, Arc}, marker::PhantomData};

use tokio::{sync::mpsc::{Sender, Receiver, channel}, time::timeout};
use tracing::{instrument, info};

use crate::{chord_id::ChordId, ChordAddress, chord::{message::{Message, PublicMessage, PrivateMessage}, ProcessorId}};

/// Connects to a local chord node as an associate to relay requests and responses.
/// 
pub struct AssociateChannel<A: ChordAddress, I: ChordId>{
	to: Sender<(ProcessorId<I>, Message<A, I>)>,
	from: Receiver<PublicMessage<A, I>>,
	associate_id: u32,
	next_associate_id: Arc<AtomicU32>,
}


impl<A: ChordAddress, I: ChordId> AssociateChannel<A, I> {

	pub(crate) fn new(associate_id:u32, to: Sender<(ProcessorId<I>, Message<A, I>)>, from: Receiver<PublicMessage<A, I>>, next_associate_id: Arc<AtomicU32>) -> Self{
		info!("creating new associate. new id is {}", associate_id);
		AssociateChannel {
			to,
			from,
			associate_id,
			next_associate_id,
		}
	}

	/// Create a new AssociateChannel from this one,
	/// that is connected to the same local chord node.
	/// 
	/// The new AssociateChannel will not share its internal channel
	/// or id with the one it was duplicated from.
	#[instrument(skip_all)]
	pub async fn duplicate(&self) -> Self {
		
		let (new_to, new_from) = channel(50);
		let new_id = self.next_associate_id.fetch_add(1, Ordering::SeqCst);
		info!("duplicating associate. new id is {}", new_id);
		self.to.send((ProcessorId::Associate(new_id), Message::Private(PrivateMessage::RegisterAssociate { conn: new_to }) )).await;
		Self {
			to: self.to.clone(),
			from: new_from,
			associate_id: new_id,
			next_associate_id: self.next_associate_id.clone(),
		}
	}


	/// Send an AssociateRequest directly.
	/// 
	/// The chord's response can be recieved later via the recv_op() method.
	/// If multiple requests are made, their responses may arrive in any
	/// order.
	pub async fn send_op(&self, msg: AssociateRequest<A, I>) {
		self.to.send((ProcessorId::Associate(self.associate_id), msg.into())).await;
	}

	/// Receive an AssociateResponse directly.
	/// 
	/// Receive a response from the chord.
	/// If multiple requests have been sent, the response received may be a
	/// response to any outstanding request.
	pub async fn recv_op(&mut self) -> Option<AssociateResponse<A, I>>{
		loop{
			let limit = tokio::time::Duration::from_secs(10);
			let msg = timeout(limit, self.from.recv()).await;
			match msg {
				Ok(Some(msg)) => {
					if let Some(msg) = msg.into() {
						return Some(msg);
					}
				},
				_ => {return None},
			}
		}
	}


}



/// Requests that can be sent to the chord via an associate connection.
pub enum AssociateRequest<A, I: ChordId>{
	/// Request the connected node's id
	GetId,
	/// Request the connected node's optional predecessor
	GetPredecessor,
	/// Request the connected node's optional successor
	GetSuccessor,

	/// Request the successor of id
	GetSuccessorOf{
		/// The id to find the successor of
		id: I
	},

	/// Request debug information
	Debug,
	#[doc(hidden)]
	Marker{data: PhantomData<A>},
}

impl<A: ChordAddress, I: ChordId> From<AssociateRequest<A, I>> for PublicMessage<A, I> {
	fn from(req: AssociateRequest<A, I>) -> Self {
		match req{
			AssociateRequest::GetId => PublicMessage::GetID,
			AssociateRequest::GetPredecessor => PublicMessage::GetPredecessor,
			AssociateRequest::GetSuccessor => PublicMessage::GetSuccessor,
			AssociateRequest::GetSuccessorOf { id } => PublicMessage::GetSuccessorOf { id },

			AssociateRequest::Debug => PublicMessage::Debug {msg: "".to_string()},
			AssociateRequest::Marker { data } => panic!("This is just a marker to quiet the compiler"),
		}
	}
}

impl<A: ChordAddress, I: ChordId> From<AssociateRequest<A, I>> for Message<A, I> {
	fn from(msg: AssociateRequest<A, I>) -> Self {
		Message::Public(msg.into())
	}
}

/// Contains the responses to requests represented by AssociateRequest.
#[derive(Debug)]
pub enum AssociateResponse<A, I: ChordId>{
	/// The id of the connected node.
	Id{
		/// The returned id.
		id: I
	},
	
	/// The predecessor of the connected node.
	Predecessor{
		/// An optional tuple of id and address,
		/// or None if the node has no predecessor.
		id: Option<(I, A)>
	},
	
	/// The successor of the connected node.
	Successor{
		/// An optional tuple of id and address,
		/// or None if the node has no successor.
		/// (Its successor would be itself)
		id: Option<(I, A)>
	},


	/// The successor of the requested id.
	SuccessorOf{
		/// Id of the successor of the requested id.
		id: I,
		/// Address of the successor of the requested id.
		addr: A
	},
	

	/// An error has occured within the chord.
	Error{
		/// More information about the error.
		msg: String
	},
	
	/// Debug message
	Debug{
		/// Currently the id, predecessor, and successor of the node
		/// followed by a list of connections to other nodes.
		msg: String
	},
}

impl<A: ChordAddress, I: ChordId> From<Message<A, I>> for Option<AssociateResponse<A, I>> {
	fn from(msg: Message<A, I>) -> Self {
		match msg{
			Message::Private(_) => None,
			Message::Public(msg) => {
				msg.into()
			},
		}
	}
}

impl<A: ChordAddress, I: ChordId> From<PublicMessage<A, I>> for Option<AssociateResponse<A, I>> {
	fn from(msg: PublicMessage<A, I>) -> Self {
		match msg{
			PublicMessage::ID { id } => Some(AssociateResponse::Id { id }),
			PublicMessage::Predecessor { pred } => Some(AssociateResponse::Predecessor { id: pred }),
			PublicMessage::Successor { succ } => Some(AssociateResponse::Successor { id: succ }),

			PublicMessage::SuccessorOf { addr, id } => Some(AssociateResponse::SuccessorOf { id, addr }),
			
			
			PublicMessage::Error { msg } => Some(AssociateResponse::Error { msg }),
			PublicMessage::Debug { msg } => Some(AssociateResponse::Debug { msg }),

			_ => None,
		}
	}
}
