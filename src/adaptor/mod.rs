use crate::{chord_processor::{ProcessorId, ChordOperation, ChordOperationResult}, chord_id::ChordId};
use tokio::{task::JoinHandle, sync::mpsc::Sender};
use serde::{Serialize, Deserialize};


pub mod tcp_adaptor;

#[derive(Serialize, Deserialize, Debug)]
pub enum ChordMessage<I: ChordId>{
    #[serde(bound = "")]
    Introduction{from: I},
    #[serde(bound = "")]
    Data{from: I, data: Vec<i8>},

    #[serde(bound = "")]
    GetPredecessor{from: I},
    #[serde(bound = "")]
    Predecessor{of: I, is: Option<I>},
    #[serde(bound = "")]
    Notify{from: I},
    #[serde(bound = "")]
    Ping{from: I},
    #[serde(bound = "")]
    Pong,
}

/// Convert the results of the chord operation to a ChordMessage.
/// This conversion can return None if a particular result
/// should not be made public.
impl<I: ChordId> From<ChordOperationResult<I>> for Option<ChordMessage<I>>{
    fn from(result: ChordOperationResult<I>) -> Self {
        match result {
			_ => None,
        }
    }
}


/// Convert incoming messages into the appropriate operations
impl<I: ChordId> From<ChordMessage<I>> for ChordOperation<I>{
    fn from(message: ChordMessage<I>) -> Self {
        match message {
            ChordMessage::Introduction { from } => todo!(),
            ChordMessage::Data { from, data } => todo!(),
            ChordMessage::GetPredecessor { from } => todo!(),
            ChordMessage::Predecessor { of, is } => todo!(),
            ChordMessage::Notify { from } => todo!(),
            ChordMessage::Ping { from } => todo!(),
            ChordMessage::Pong => todo!(),
        }
    }
}


pub trait ChordAdaptor<A, I: ChordId>{

	fn new() -> Self;

	// incoming connections
	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, ChordOperation<I>)>) -> JoinHandle<()>;

	// create outgoing connection
	// fn connect();
}

