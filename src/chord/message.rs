use serde::{Serialize, Deserialize};
use tokio::sync::mpsc::Sender;

use crate::{ChordAddress, chord_id::ChordId};

use super::ProcessorId;



#[derive(Debug)]
pub enum Message<A: ChordAddress, I: ChordId>{
	Private(PrivateMessage<A, I>),
	Public(PublicMessage<A, I>),
}

#[derive(Debug)]
pub enum PrivateMessage<A: ChordAddress, I: ChordId>{
	// Internal Operations
	RegisterMember{addr: A, conn: Sender<PublicMessage<A, I>>},
	RegisterAssociate{conn: Sender<PublicMessage<A, I>>},

	// Timed Operation Triggers
	Stabilize,
	FixFingers,
	CheckPredecessor,
	Cleanup,
	SaveState,
}

impl<A: ChordAddress, I: ChordId> From<PrivateMessage<A, I>> for Message<A, I> {
	fn from(msg: PrivateMessage<A, I>) -> Self {
		Message::Private(msg)
	}
}


#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum PublicMessage<A: ChordAddress, I: ChordId>{
	Introduction{id: I, addr: A},

	// State Operations
	GetID,
	ID{id: I},
	GetPredecessor,
	Predecessor{pred: Option<(I, A)>},
	GetSuccessor,
	Successor{succ: Option<(I, A)>},

	

	// Chord Operations
	Route{packet: Packet<A, I>},
	GetSuccessorOf{id: I},
	SuccessorOf{id: I, addr: A},
	Notify,

	// Other
	GetAdvertOf{id: I},
	AdvertOf{data: Option<Vec<u8>>},

	Error{msg: String},
	Debug{msg: String},
}

impl<A: ChordAddress, I: ChordId> From<PublicMessage<A, I>> for Message<A, I> {
	fn from(msg: PublicMessage<A, I>) -> Self {
		Message::Public(msg)
	}
}



#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Packet<A: ChordAddress, I: ChordId>{
	pub to: I,
	pub from: I,
	pub channel: ProcessorId<I>,
	pub exact: bool,
	pub checksum: u32,
	pub packet_type: PacketType<A, I>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum PacketType<A: ChordAddress, I: ChordId>{
	GetSuccessorOf{id: I},
	SuccessorOf{id: I, addr: A},
	Dialback{id: I, addr: A},
	GetAdvert,
	Advert{data:Option<Vec<u8>>},
	Error{msg: String}
}


impl<A: ChordAddress, I: ChordId> Packet<A, I>{
	pub fn new(to: I, from: I, channel: ProcessorId<I>, exact: bool, packet_type: PacketType<A, I>) -> Self {
		Self{
			to,
			from,
			channel,
			exact,
			checksum: 50, // temp value
			packet_type,
		}
	}

	pub fn reply_with(&self, from: &I, packet_type: PacketType<A, I>) -> Self{
		Self{
			to: self.from.clone(),
			from: from.clone(),
			channel: self.channel.clone(),
			exact: true,
			checksum: self.checksum,
			packet_type,
		}
	}
}





