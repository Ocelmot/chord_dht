use std::{io::{ErrorKind, Result}, sync::{Arc, atomic::{AtomicU32, Ordering}}, future::Future};

use super::{ChordAdaptor, ChordMessage};
use crate::{ChordId, chord_processor::{ProcessorId, ChordOperation, ChordOperationResult}};

use serde_json::Deserializer;
use tokio::{net::{ToSocketAddrs, TcpListener, TcpStream}, sync::mpsc::{Sender, self, Receiver}, task::JoinHandle, io::{AsyncWriteExt, AsyncReadExt}, select};



pub struct TCPAdaptor{
	next_associate_id: Arc<AtomicU32>,
}

impl<A: ToSocketAddrs + Send + 'static, I: ChordId> ChordAdaptor<A, I> for TCPAdaptor{
	
	fn new() -> Self{
		Self{
			next_associate_id: Arc::new(AtomicU32::new(1)),
		}
	}

	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, ChordOperation<A, I>)>) -> JoinHandle<()> {
		let next_associate_id = self.next_associate_id.clone();
		tokio::spawn(async move{
			let listener = TcpListener::bind(listen_addr).await.expect("listener should not fail");
			loop{
				match listener.accept().await {
					Err(e) => { 
						/* TODO: probably shut down listener */
						eprintln!("Encountered an error in accept: {}", e)
					},
					Ok((stream, _)) => {
						let mut ch_stream = TcpChordStream::<I>::new(stream);
						let id = match ch_stream.peek().await {
							Ok(ChordMessage::Introduction { from }) => {
								ProcessorId::Member(from.clone())
							},
							_ => {
								let next_id = next_associate_id.fetch_add(1, Ordering::SeqCst);
								ProcessorId::Associate(next_id)
							}
						};
						let (inner_tx, mut inner_rx) = mpsc::channel(50);
						Self::adapt(id.clone(), ch_stream, channel.clone(), inner_rx);
						channel.send((id.clone(), ChordOperation::RegisterConnection { conn: inner_tx })).await;
					}
				}
			}
		})
    }

	fn connect(&self, addr: A, id: Option<I>, channel_from_connection: Sender<(ProcessorId<I>, ChordOperation<A, I>)>) -> Sender<ChordOperationResult<A, I>> {
		let (inner_tx, mut inner_rx) = mpsc::channel(50);
		let next_associate_id = self.next_associate_id.clone();
		tokio::spawn(async move {
			let conn = TcpStream::connect(addr).await.expect("Failed to connect");
			let stream = TcpChordStream::<I>::new(conn);
			let processor_id = match id {
				Some(id) => ProcessorId::Member(id),
				None => ProcessorId::Associate(next_associate_id.fetch_add(1, Ordering::SeqCst)),
			};
			TCPAdaptor::adapt(processor_id, stream, channel_from_connection, inner_rx);
		});
		return inner_tx;
	}

	fn associate_connect(addr: A) -> (Sender<ChordOperation<A, I>>, Receiver<ChordOperationResult<A, I>>) {
		let (to_tx, mut to_rx) = mpsc::channel(50);
		let (from_tx, mut from_rx) = mpsc::channel(50);
		tokio::spawn(async move{
			let stream = TcpStream::connect(addr).await.expect("failed to connect");
			let mut stream = TcpChordStream::<I>::new(stream);
			loop{
				// select on reading from stream and reading from created channel
				select! {
					// if stream completes, pass operation to channel
					incoming = stream.read() => {
						match incoming{
							Ok(incoming) => {
								from_tx.send(incoming.into()).await;
							},
							Err(_) => todo!("Failed to read from incoming data stream"),
						}
					},
					// if created channel completes, write to stream
					outgoing = to_rx.recv() => {
						match outgoing {
							Some(chord_result) => {
								if let Some(msg) = chord_result.into(){
									stream.write(msg).await;
								}
							},
							None => todo!("Failed to read message from processor"),
						}
					},
				}
			}
		});
		(to_tx, from_rx)
	}
}


impl TCPAdaptor{
	fn adapt<A: Send + 'static, I: ChordId>(id: ProcessorId<I>, mut stream: TcpChordStream<I>, channel_to_processor: Sender<(ProcessorId<I>, ChordOperation<A, I>)>, mut channel_from_processor: Receiver<ChordOperationResult<A, I>>){
		tokio::spawn(async move{
			loop{
				// select on reading from stream and reading from created channel
				select! {
					// if stream completes, pass operation to channel
					incoming = stream.read() => {
						match incoming{
							Ok(incoming) => {
								channel_to_processor.send((id.clone(), incoming.into())).await;
							},
							Err(_) => todo!("Failed to read from incoming data stream"),
						}
					},
					// if created channel completes, write to stream
					outgoing = channel_from_processor.recv() => {
						match outgoing {
							Some(chord_result) => {
								if let Some(msg) = chord_result.into(){
									stream.write(msg).await;
								}
							},
							None => todo!("Failed to read message from processor"),
						}
					},
				}
			}
		});
	}
}


struct TcpChordStream<I: ChordId>{
	stream: TcpStream,
	buffer: Vec<u8>,
	peaked: Option<ChordMessage<I>>
}
impl<I: ChordId> TcpChordStream<I>{
	pub fn new(stream: TcpStream) -> Self{
		Self{
			stream,
			buffer: Vec::new(),
			peaked: None,
		}
	}

	async fn read(&mut self) -> Result<ChordMessage<I>>{
		if let Some(msg) = self.peaked.take() {
			return Ok(msg);
		}
		loop{
			// attempt to deserialize buffer
			let mut deserializer = Deserializer::from_slice(self.buffer.as_slice()).into_iter();

			// if successful, truncate buffer, return deserialized struct
	        for result in &mut deserializer{
				match result{
					Ok(msg) => {
						self.buffer = self.buffer[deserializer.byte_offset()..].to_vec();
						return Ok(msg);
					},
					Err(e) => {
						eprintln!("Encountered deserialization error: {}", e);
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
					continue;
				}
			}
		}
	}

	async fn write(&mut self, msg: ChordMessage<I>){
		let raw_data = serde_json::ser::to_string(&msg).expect("Failed to serialize struct");
		self.stream.write(raw_data.as_bytes()).await;
	}

	async fn peek(&mut self) -> Result<&mut ChordMessage<I>>{
		let msg = self.read().await?;
		self.peaked = Some(msg);
		Ok(self.peaked.as_mut().unwrap())
	}
}
