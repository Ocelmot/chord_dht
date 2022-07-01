use crate::{chord_processor::{ProcessorId, ChordOperation, ChordOperationResult}, chord_id::ChordId};

use tokio::{task::JoinHandle, sync::mpsc::{Sender, Receiver}};
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
impl<A, I: ChordId> From<ChordOperationResult<A, I>> for Option<ChordMessage<I>>{
    fn from(result: ChordOperationResult<A, I>) -> Self {
        match result {
			_ => None,
        }
    }
}


/// Convert incoming messages into the appropriate operations
impl<A, I: ChordId> From<ChordMessage<I>> for ChordOperation<A, I>{
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


pub trait ChordAdaptor<A, I: ChordId>: Send + 'static{

	fn new() -> Self;

	// incoming connections
	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, ChordOperation<A, I>)>) -> JoinHandle<()>;

	// create outgoing connection
	fn connect(&self, addr: A, id: Option<I>, channel_from_connection: Sender<(ProcessorId<I>, ChordOperation<A, I>)>) -> Sender<ChordOperationResult<A, I>>;

    // connect at an associate
    fn associate_connect(addr: A) -> (Sender<ChordOperation<A, I>>, Receiver<ChordOperationResult<A, I>>);
}

