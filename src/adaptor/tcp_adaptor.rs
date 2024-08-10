use std::{
    marker::PhantomData,
    result::Result,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use super::{AssociateClient, ChordAdaptor, Message};
use crate::{
    adaptor::AssociateProtocol,
    chord::{
        message::{PrivateMessage, PublicMessage},
        ProcessorId,
    },
    error::{ChordError, ChordResult, ErrorKind},
    ChordAddress, ChordId,
};

use serde_json::{error::Category, Deserializer};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    select,
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::error;

/// An implementation of ChordAdaptor that creates TCP connections from any address type that implements ToSocketAddrs.
#[derive(Debug)]
pub struct TCPAdaptor<A, I> {
    next_associate_id: Arc<AtomicU32>,
    id: PhantomData<I>,
    addr: PhantomData<A>,
}

impl<A: ChordAddress + ToSocketAddrs, I: ChordId> ChordAdaptor<A, I> for TCPAdaptor<A, I> {
    type Error = ChordError;

    fn new(next_associate_id: Arc<AtomicU32>) -> Result<TCPAdaptor<A, I>, ChordError> {
        Ok(Self {
            next_associate_id,
            id: PhantomData,
            addr: PhantomData,
        })
    }

    fn listen_handler(
        &self,
        listen_addr: A,
        channel: Sender<(ProcessorId<I>, Message<A, I>)>,
    ) -> Result<Option<JoinHandle<Result<(), ChordError>>>, ChordError> {
        let next_associate_id = self.next_associate_id.clone();
        let handle = tokio::spawn(async move {
            let listener = TcpListener::bind(listen_addr)
                .await
                .expect("listener should not fail");
            loop {
                match listener.accept().await {
                    Err(e) => {
                        /* TODO: probably shut down listener */
                        panic!("Encountered an error in accept: {}", e)
                    }
                    Ok((stream, _)) => {
                        let mut ch_stream = TcpChordStream::<A, I>::new(stream);
                        let (inner_tx, inner_rx) = mpsc::channel(50);
                        let (id, message) = match ch_stream.peek().await {
                            Ok(AssociateProtocol::Message(PublicMessage::Introduction {
                                id,
                                addr,
                            })) => {
                                let res = (
                                    ProcessorId::Member(id.clone()),
                                    PrivateMessage::RegisterMember {
                                        addr: addr.clone(),
                                        conn: inner_tx,
                                    },
                                );
                                ch_stream.read().await?;
                                res
                            }
                            _ => {
                                let next_id = next_associate_id.fetch_add(1, Ordering::SeqCst);
                                (
                                    ProcessorId::Associate(next_id),
                                    PrivateMessage::RegisterAssociate { conn: inner_tx },
                                )
                            }
                        };

                        Self::adapt(id.clone(), ch_stream, channel.clone(), inner_rx);
                        channel
                            .send((id.clone(), Message::Private(message)))
                            .await?;
                    }
                }
            }
        });
        Ok(Some(handle))
    }

    fn connect(
        &self,
        addr: A,
        as_id: Option<I>,
        channel_from_connection: Sender<(ProcessorId<I>, Message<A, I>)>,
    ) -> Result<Sender<PublicMessage<A, I>>, ChordError> {
        let (inner_tx, inner_rx) = mpsc::channel(50);
        let next_associate_id = self.next_associate_id.clone();
        tokio::spawn(async move {
            let conn = TcpStream::connect(addr).await.expect("Failed to connect");
            let stream = TcpChordStream::<A, I>::new(conn);
            let processor_id = match as_id {
                Some(id) => ProcessorId::Member(id),
                None => ProcessorId::Associate(next_associate_id.fetch_add(1, Ordering::SeqCst)),
            };
            TCPAdaptor::adapt(processor_id, stream, channel_from_connection, inner_rx);
        });

        return Ok(inner_tx);
    }

    fn associate_client(addr: A) -> Result<AssociateClient<A, I>, ChordError> {
        let (to_tx, mut to_rx) = mpsc::channel(50);
        let (from_tx, from_rx) = mpsc::channel(50);
        tokio::spawn(async move {
            let stream = match TcpStream::connect(addr.clone()).await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("failed to connect to {:?} with error {}", addr, e);
                    return;
                }
            };
            let mut stream = TcpChordStream::<A, I>::new(stream);
            loop {
                // select on reading from stream and reading from created channel
                select! {
                    // if stream completes, pass operation to channel
                    incoming = stream.read() => {
                        match incoming{
                            Ok(incoming) => {
                                if from_tx.send(incoming).await.is_err() {
                                    break;
                                }
                            },
                            Err(_) => break,
                        }
                    },
                    // if created channel completes, write to stream
                    outgoing = to_rx.recv() => {

                        match outgoing {
                            Some(chord_result) => {
                                println!("about to send {:?}", chord_result);
                                if let Some(msg) = Option::<AssociateProtocol<A, I>>::from(chord_result){
                                    if stream.write(msg).await.is_err(){
                                        break;
                                    }
                                }
                            },
                            None => break,
                        }
                    },
                }
            }
        });
        Ok(AssociateClient::new(to_tx, from_rx))
    }
}

impl<A: ChordAddress, I: ChordId> TCPAdaptor<A, I> {
    fn adapt(
        id: ProcessorId<I>,
        mut stream: TcpChordStream<A, I>,
        channel_to_processor: Sender<(ProcessorId<I>, Message<A, I>)>,
        mut channel_from_processor: Receiver<PublicMessage<A, I>>,
    ) {
        tokio::spawn(async move {
            loop {
                // select on reading from stream and reading from created channel
                select! {
                    // if stream completes, pass operation to channel
                    incoming = stream.read() => {
                        match incoming{
                            Ok(incoming) => {

                                match incoming {
                                    AssociateProtocol::Message(msg) => {
                                        if channel_to_processor.send((id.clone(), Message::Public(msg))).await.is_err() {
                                            break;
                                        }
                                    },
                                    AssociateProtocol::GetPublicAddr => {
                                        let addr = stream.get_peer_address();
                                        if stream.write(AssociateProtocol::PublicAddr{addr}).await.is_err() {
                                            break;
                                        }
                                    },
                                    // Will never request a public address, skip processing responses
                                    AssociateProtocol::PublicAddr { .. } => {},
                                }
                            },
                            Err(_) => break,
                        }
                    },
                    // if created channel completes, write to stream
                    outgoing = channel_from_processor.recv() => {
                        match outgoing {
                            Some(msg) => {
                                if stream.write(AssociateProtocol::Message(msg)).await.is_err(){
                                    break;
                                }
                            },
                            None => break,
                        }
                    },
                }
            }
        });
    }
}

struct TcpChordStream<A: ChordAddress, I: ChordId> {
    stream: TcpStream,
    buffer: Vec<u8>,
    peaked: Option<AssociateProtocol<A, I>>,
}
impl<A: ChordAddress, I: ChordId> TcpChordStream<A, I> {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
            peaked: None,
        }
    }

    async fn read(&mut self) -> ChordResult<AssociateProtocol<A, I>> {
        if let Some(msg) = self.peaked.take() {
            // println!("Returning peaked message");
            return Ok(msg);
        }
        loop {
            // println!("about to deserialize from buffer");
            // attempt to deserialize buffer
            let mut deserializer = Deserializer::from_slice(self.buffer.as_slice()).into_iter();

            // if successful, truncate buffer, return deserialized struct
            for result in &mut deserializer {
                match result {
                    Ok(msg) => {
                        self.buffer = self.buffer[deserializer.byte_offset()..].to_vec();
                        return Ok(msg);
                    }
                    Err(ref e) if e.classify() == Category::Eof => {
                        break; // if we have encountered an EOF, more information may arrive later
                    }
                    Err(e) => {
                        eprintln!("Encountered deserialization error: {}", e);
                        eprintln!(
                            "\t Deserialization buffer: {:?}",
                            String::from_utf8(self.buffer.clone()).unwrap()
                        );
                    }
                }
            }

            // else, read bytes into buffer
            let mut tmp_buf = vec![0; 1024];
            match self.stream.read(&mut tmp_buf).await {
                Ok(0) => {
                    // End of stream, no more data
                    return Err(ErrorKind::AssociateClosed)?;
                }
                Ok(len) => {
                    // Append data to buffer
                    self.buffer.extend_from_slice(&tmp_buf[..len]);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue; // try to read again
                }
                Err(e) => {
                    return Err(ChordError::from(e).problem(ErrorKind::AssociateClosed));
                }
            }
        }
    }

    async fn write(&mut self, msg: AssociateProtocol<A, I>) -> ChordResult {
        let raw_data = serde_json::ser::to_string(&msg).expect("Failed to serialize struct");
        self.stream.write(raw_data.as_bytes()).await?;
        Ok(())
    }

    async fn peek(&mut self) -> ChordResult<&mut AssociateProtocol<A, I>> {
        let msg = self.read().await?;
        self.peaked = Some(msg);
        Ok(self.peaked.as_mut().unwrap())
    }

    fn get_peer_address(&self) -> Option<String> {
        match self.stream.peer_addr() {
            Ok(addr) => Some(addr.to_string()),
            Err(_) => None,
        }
    }
}
