use std::{io::{ErrorKind, Result}, sync::{Arc, atomic::{AtomicU32, Ordering}}};

use super::{ChordAdaptor, Message, AssociateClient};
use crate::{ChordId, ChordAddress, chord::{ProcessorId, message::{PrivateMessage, PublicMessage}}, adaptor::AssociateProtocol};

use serde_json::{Deserializer, error::Category};
use tokio::{net::{ToSocketAddrs, TcpListener, TcpStream}, sync::mpsc::{Sender, self, Receiver}, task::JoinHandle, io::{AsyncWriteExt, AsyncReadExt}, select};

/// An implementation of ChordAdaptor that creates TCP connections from any address type that implements ToSocketAddrs.
#[derive(Debug)]
pub struct TCPAdaptor<A, I>{
	id: I,
	addr: A,
	next_associate_id: Arc<AtomicU32>,

}

impl<A: ChordAddress + ToSocketAddrs, I: ChordId> ChordAdaptor<A, I> for TCPAdaptor<A, I>{
	
	fn new(id: I, addr: A, next_associate_id: Arc<AtomicU32>) -> Self{
		Self{
			id,
			addr,
			next_associate_id,
		}
	}

	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, Message<A, I>)>) -> JoinHandle<()> {
		let next_associate_id = self.next_associate_id.clone();
		tokio::spawn(async move{
			let listener = TcpListener::bind(listen_addr).await.expect("listener should not fail");
			loop{
				match listener.accept().await {
					Err(e) => { 
						/* TODO: probably shut down listener */
						panic!("Encountered an error in accept: {}", e)
					},
					Ok((stream, _)) => {
						let mut ch_stream = TcpChordStream::<A, I>::new(stream);
						let (inner_tx, inner_rx) = mpsc::channel(50);
						let (id, message) = match ch_stream.peek().await {
							Ok(AssociateProtocol::Message(PublicMessage::Introduction{id, addr})) => {
								let res = (ProcessorId::Member(id.clone()), PrivateMessage::RegisterMember { addr:addr.clone(), conn: inner_tx });
								ch_stream.read().await;
								res
							},
							_ => {
								let next_id = next_associate_id.fetch_add(1, Ordering::SeqCst);
								(ProcessorId::Associate(next_id), PrivateMessage::RegisterAssociate { conn: inner_tx })
							}
						};
						
						Self::adapt(id.clone(), ch_stream, channel.clone(), inner_rx);
						channel.send((id.clone(), Message::Private(message))).await;
					}
				}
			}
		})
	}

	fn connect(&self, addr: A, as_id: Option<I>, channel_from_connection: Sender<(ProcessorId<I>, Message<A, I>)>) -> Sender<PublicMessage<A, I>> {
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
		
		return inner_tx;
	}

	fn associate_client(addr: A) -> AssociateClient<A, I> {
		let (to_tx, mut to_rx) = mpsc::channel(50);
		let (from_tx, from_rx) = mpsc::channel(50);
		tokio::spawn(async move{
			let stream = TcpStream::connect(addr).await.expect("failed to connect");
			let mut stream = TcpChordStream::<A, I>::new(stream);
			loop{
				// select on reading from stream and reading from created channel
				select! {
					// if stream completes, pass operation to channel
					incoming = stream.read() => {
						match incoming{
							Ok(incoming) => {
								from_tx.send(incoming).await;
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
									stream.write(msg).await;
								}
							},
							None => break,
						}
					},
				}
			}
		});
		AssociateClient::new(to_tx, from_rx)
	}
}


impl<A: ChordAddress, I: ChordId> TCPAdaptor<A, I>{
	fn adapt(id: ProcessorId<I>, mut stream: TcpChordStream<A, I>, channel_to_processor: Sender<(ProcessorId<I>, Message<A, I>)>, mut channel_from_processor: Receiver<PublicMessage<A, I>>){
		tokio::spawn(async move{
			loop{
				// select on reading from stream and reading from created channel
				select! {
					// if stream completes, pass operation to channel
					incoming = stream.read() => {
						match incoming{
							Ok(incoming) => {
								
								match incoming {
									AssociateProtocol::Message(msg) => {
										channel_to_processor.send((id.clone(), Message::Public(msg))).await;
									},
									AssociateProtocol::GetPublicAddr => {
										let addr = stream.get_peer_address();
										stream.write(AssociateProtocol::PublicAddr{addr}).await;
									},
									// Will never request a public address, skip processing responses
									AssociateProtocol::PublicAddr { addr } => {},
								}
							},
							Err(_) => break,
						}
					},
					// if created channel completes, write to stream
					outgoing = channel_from_processor.recv() => {
						match outgoing {
							Some(msg) => {
								stream.write(AssociateProtocol::Message(msg)).await;
							},
							None => break,
						}
					},
				}
			}
		});
	}
}


struct TcpChordStream<A: ChordAddress, I: ChordId>{
	stream: TcpStream,
	buffer: Vec<u8>,
	peaked: Option<AssociateProtocol<A, I>>
}
impl<A: ChordAddress, I: ChordId> TcpChordStream<A, I>{
	pub fn new(stream: TcpStream) -> Self{
		Self{
			stream,
			buffer: Vec::new(),
			peaked: None,
		}
	}

	async fn read(&mut self) -> Result<AssociateProtocol<A, I>>{
		if let Some(msg) = self.peaked.take() {
			// println!("Returning peaked message");
			return Ok(msg);
		}
		loop{
			// println!("about to deserialize from buffer");
			// attempt to deserialize buffer
			let mut deserializer = Deserializer::from_slice(self.buffer.as_slice()).into_iter();

			// if successful, truncate buffer, return deserialized struct
			for result in &mut deserializer{
				match result{
					Ok(msg) => {
						self.buffer = self.buffer[deserializer.byte_offset()..].to_vec();
						return Ok(msg);
					},
					Err(ref e) if e.classify() == Category::Eof => {
						break; // if we have encountered an EOF, more information may arrive later
					},
					Err(e) => {
						eprintln!("Encountered deserialization error: {}", e);
						eprintln!("\t Deserialization buffer: {:?}", String::from_utf8(self.buffer.clone()).unwrap());
					},
				}
			}
			
			// else, read bytes into buffer
			let mut tmp_buf = vec![0; 1024];
			match self.stream.read(&mut tmp_buf).await {
				Ok(0) => { 
					return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)); // No more data
				}, 
				Ok(len) => { // Append data to buffer
					self.buffer.extend_from_slice(&tmp_buf[..len]);
				},
				Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
					continue; // try to read again
				},
				Err(e) =>{
					println!("Encountered error reading from connection: {}", e);
					
					// probably should terminate connection here, depending on error
					break Err(e);
				}
			}
		}
	}

	async fn write(&mut self, msg: AssociateProtocol<A, I>){
		let raw_data = serde_json::ser::to_string(&msg).expect("Failed to serialize struct");
		self.stream.write(raw_data.as_bytes()).await;
	}

	async fn peek(&mut self) -> Result<&mut AssociateProtocol<A, I>>{
		let msg = self.read().await?;
		self.peaked = Some(msg);
		Ok(self.peaked.as_mut().unwrap())
	}

	fn get_peer_address(&self) -> Option<String>{
		match self.stream.peer_addr() {
			Ok(addr) => Some(addr.to_string()),
			Err(_) => None,
		}
	}
}
