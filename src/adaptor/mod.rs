use crate::{
    chord_id::ChordId,
    associate::{AssociateRequest, AssociateResponse},
    ChordAddress, chord::{message::{Message, PublicMessage}, ProcessorId}
};

use std::fmt::Debug;

use tokio::{task::JoinHandle, sync::mpsc::{Sender, Receiver}};


pub mod tcp_adaptor;


pub trait ChordAdaptor<A: ChordAddress, I: ChordId>: Send + Sync + 'static + Debug{

	fn new(id: I, addr: A) -> Self;

	// incoming connections
	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, Message<A, I>)>) -> JoinHandle<()>;

	// create outgoing connection
	fn connect(&self, addr: A, id: Option<I>, channel_from_connection: Sender<(ProcessorId<I>, Message<A, I>)>) -> Sender<PublicMessage<A, I>>;

    // connect at an associate
    fn associate_client(addr: A) -> AssociateClient<A, I>;
}

pub struct AssociateClient<A: ChordAddress, I: ChordId>{
    to: Sender<PublicMessage<A, I>>,
    from: Receiver<PublicMessage<A, I>>,
}

impl<A: ChordAddress, I: ChordId> AssociateClient<A, I> {
    pub fn new(to: Sender<PublicMessage<A, I>>, from: Receiver<PublicMessage<A, I>>) -> Self{
        AssociateClient {
            to,
            from
        }
    }

    pub async fn send_op(&self, msg: AssociateRequest<A, I>) {
        self.to.send(msg.into()).await;
    }

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