use crate::{
    associate::{AssociateRequest, AssociateResponse},
    chord::{
        message::{Message, PublicMessage},
        ProcessorId,
    },
    chord_id::ChordId,
    error::{ChordResult, ErrorKind, Problem},
    ChordAddress,
};

use std::{
    error::Error,
    fmt::Debug,
    sync::{atomic::AtomicU32, Arc},
};

use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

/// An implementation of ChordAdaptor for address types that implement ToSocketAddrs
pub mod tcp_adaptor;

/// Errors from a chord adaptor must implement Error, Send, and Sync.
/// This trait describes that.
pub trait AdaptorError: Error + Send + Sync + 'static {}
impl<T: Error + Send + Sync + 'static> AdaptorError for T {}

/// A ChordAdaptor is instantiated within a chord node to allow the node listen
/// for incoming connections and to convert addresses to outgoing connections.
///
/// Implementing this trait allows the chord logic to be implemented for other
/// protocols or types of connections.
pub trait ChordAdaptor<A: ChordAddress, I: ChordId>: Sized + Send + Sync + 'static + Debug {
    /// The error type an implementation of ChordAdaptor must return
    type Error: AdaptorError;

    /// Create a new ChordAdaptor instance
    fn new(next_associate_id: Arc<AtomicU32>) -> Result<Self, Self::Error>;

    /// Spawn a task to listen for new connections to listen_addr.
    /// New channels are then registered through the provided sender channel.
    fn listen_handler(
        &self,
        listen_addr: A,
        channel: Sender<(ProcessorId<I>, Message<A, I>)>,
    ) -> Result<Option<JoinHandle<Result<(), Self::Error>>>, Self::Error>;

    /// Return the Sender portion of a channel that connects to a node at addr.
    /// If an id is provided, the connection is a member connection, otherwise
    /// it is an associate connection.
    /// Incoming messages should be routed to the provided Sender.
    fn connect(
        &self,
        addr: A,
        id: Option<I>,
        channel_from_connection: Sender<(ProcessorId<I>, Message<A, I>)>,
    ) -> Result<Sender<PublicMessage<A, I>>, Self::Error>;

    /// Create a connection to the node at addr as an associate.
    /// Return an AssociateClient representing that connection.
    fn associate_client(addr: A) -> Result<AssociateClient<A, I>, Self::Error>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
enum AssociateProtocol<A: ChordAddress, I: ChordId> {
    Message(PublicMessage<A, I>),
    GetPublicAddr,
    PublicAddr { addr: Option<String> },
}

/// An AssociateClient behaves very similarly to an AssociateChannel,
/// the distinction being that it is connected to a remote node at a
/// particular address rather than a node spawned on the current
/// machine.
pub struct AssociateClient<A: ChordAddress, I: ChordId> {
    to: Sender<AssociateProtocol<A, I>>,
    from: Receiver<AssociateProtocol<A, I>>,
}

impl<A: ChordAddress, I: ChordId> AssociateClient<A, I> {
    /// Create an AssociateClient from two streams, a Sender and a Receiver.
    fn new(to: Sender<AssociateProtocol<A, I>>, from: Receiver<AssociateProtocol<A, I>>) -> Self {
        AssociateClient { to, from }
    }

    /// Send an AssociateRequest directly.
    ///
    /// The chord's response can be recieved later via the recv_op() method.
    /// If multiple requests are made, their responses may arrive in any
    /// order.
    pub async fn send_op(&self, msg: AssociateRequest<A, I>) -> ChordResult {
        self.to.send(AssociateProtocol::Message(msg.into())).await?;
        Ok(())
    }

    /// Receive an AssociateResponse directly.
    ///
    /// Receive a response from the chord.
    /// If multiple requests have been sent, the response received may be a
    /// response to any outstanding request.
    pub async fn recv_op(&mut self) -> Option<AssociateResponse<A, I>> {
        loop {
            let msg = self.from.recv().await;
            match msg {
                Some(prot) => {
                    match prot {
                        AssociateProtocol::Message(msg) => {
                            if let Some(msg) = msg.into() {
                                return Some(msg);
                            }
                        }
                        AssociateProtocol::GetPublicAddr => {} // should never happen
                        AssociateProtocol::PublicAddr { addr: _ } => {
                            return None;
                        }
                    }
                }
                None => return None,
            }
        }
    }

    /// Query for the successor of a particular node,
    /// and await the response as a single operation.
    /// If other responses arrive, This is returned as
    /// [ErrorKind::MismatchedResponse].
    pub async fn successor_of(&mut self, id: I) -> ChordResult<(I, A)> {
        self.send_op(AssociateRequest::GetSuccessorOf { id })
            .await
            .problem(ErrorKind::AssociateClosed)?;
        let response = self.recv_op().await;
        match response {
            Some(AssociateResponse::SuccessorOf { id, addr }) => {
                return Ok((id, addr));
            }
            Some(_) => Err(ErrorKind::MismatchedResponse)?,
            None => Err(ErrorKind::AssociateClosed)?,
        }
    }

    /// Query for the connected node to return the address
    /// it percieves as belonging to this connection.
    /// Useful in determining the public address.
    pub async fn public_address(&mut self) -> ChordResult<Option<String>> {
        self.to
            .send(AssociateProtocol::GetPublicAddr)
            .await
            .problem(ErrorKind::AssociateClosed)?;
        let response = self.from.recv().await;
        match response {
            Some(AssociateProtocol::PublicAddr { addr }) => {
                return Ok(addr);
            }
            Some(_) => Err(ErrorKind::MismatchedResponse)?,
            None => Err(ErrorKind::AssociateClosed)?,
        }
    }
}
