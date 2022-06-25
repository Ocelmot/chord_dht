use std::{io::{ErrorKind, Result}, sync::{Arc, atomic::{AtomicU32, Ordering}}};

use super::{ChordAdaptor, ChordMessage};
use crate::{ChordId, chord_processor::{ProcessorId, ChordOperation}};

use serde_json::Deserializer;
use tokio::{net::{ToSocketAddrs, TcpListener, TcpStream}, sync::mpsc::{Sender, self}, task::JoinHandle, io::{AsyncWriteExt, AsyncReadExt}, select};



pub struct TCPAdaptor{
	next_associate_id: Arc<AtomicU32>,
}

impl<A: ToSocketAddrs + Send + 'static, I: ChordId> ChordAdaptor<A, I> for TCPAdaptor{
	fn new() -> Self{
		Self{
			next_associate_id: Arc::new(AtomicU32::new(1)),
		}
	}
	fn listen_handler(&self, listen_addr: A, channel: Sender<(ProcessorId<I>, ChordOperation<I>)>) -> JoinHandle<()> {
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
						Self::adapt(id, ch_stream, channel.clone());
					}
				}
			}
		})
    }
}


impl TCPAdaptor{
	fn adapt<I: ChordId>(id: ProcessorId<I>, mut stream: TcpChordStream<I>, channel: Sender<(ProcessorId<I>, ChordOperation<I>)>){
		tokio::spawn(async move{
			// create channel
			let (inner_tx, mut inner_rx) = mpsc::channel(50);
			// send tx to chord processor
			channel.send((id.clone(), ChordOperation::RegisterConnection { conn: inner_tx })).await;

			// select on reading from stream and reading from created channel
			select! {
				// if stream completes, pass operation to channel
				incoming = stream.read() => {
					match incoming{
						Ok(incoming) => {
							channel.send((id, incoming.into()));
						},
						Err(_) => todo!("Failed to read from incoming data stream"),
					}
				},
				// if created channel completes, write to stream
				outgoing = inner_rx.recv() => {
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





//////////// BELOW IS CODE FROM A PREVIOUS IMPLEMENTATION, TAKE SERIALIZATION FROM THIS



// use std::{io::ErrorKind, sync::Arc};
// use chrono::{DateTime, Utc};

// use serde_json::Deserializer;
// use tokio::{net::{TcpStream, tcp::{OwnedWriteHalf, OwnedReadHalf}, ToSocketAddrs}, io::{AsyncWriteExt, AsyncReadExt}, task::JoinHandle, sync::{RwLock, mpsc::Sender}};

// use crate::{chord_msg::{ChordMessage, RawMessage}, processor_data::ChordOperation, chord_id::ChordId};

// #[derive(Debug)]
// pub enum Node<I: ChordId>{
//     Local{id: I},
//     Other{
//         id: I,
//         write_half: OwnedWriteHalf,
//         read_handle: JoinHandle<()>,
//         last_msg: Arc<RwLock<DateTime<Utc>>>,
//     }
// }

// impl<I: ChordId> Node<I>{

//     pub async fn connect<A, F: 'static>(id: I, addr: A, sender: Sender<ChordOperation<I>>, on_recv: F) -> Result<Node<I>, std::io::Error>
//     where A: ToSocketAddrs,
//         F: FnMut(ChordMessage<I>) -> Option<ChordOperation<I>> + std::marker::Send{

//         let stream = TcpStream::connect(addr).await?;

//         let (read_half, write_half) = stream.into_split();

//         let last_msg = Arc::new(RwLock::new(Utc::now()));

//         let read_handle = read_handler(read_half, Vec::new(), on_recv, sender, last_msg.clone()).await; 


//         let mut res = Node::Other{
//             id: id.clone(),
//             write_half,
//             read_handle,
//             last_msg,
//         };
//         res.send(ChordMessage::Introduction{from: id}).await;
//         Ok(res)
//     }

//     pub async fn from_stream<F: 'static>(stream: TcpStream, sender: Sender<ChordOperation<I>>, on_recv: F) -> Result<Node<I>, std::io::Error>
//     where F: FnMut(ChordMessage<I>) -> Option<ChordOperation<I>> + std::marker::Send{
//         // listen over socket until the stream has introduced itself
//         // then use that id to init the Node
//         let (mut read_half, write_half) = stream.into_split();

//         let mut buffer = Vec::new();
        
//         let intro = read_message(&mut read_half, &mut buffer).await?;
        
//         let id = if let ChordMessage::Introduction{from} = intro{
//             from
//         } else {
//             return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
//         };

//         let last_msg = Arc::new(RwLock::new(Utc::now()));

//         let read_handle =  read_handler(read_half, buffer, on_recv, sender, last_msg.clone()).await;


//         Ok(Node::Other{id, write_half, read_handle, last_msg})
//     }

//     pub async fn send_raw(&mut self, message: RawMessage<I>) -> Result<(), std::io::Error>{
//         match self{
//             Node::Local{..} => todo!(),
//             Node::Other { id, write_half, read_handle, last_msg } => {
                
//                 let raw_data = serde_json::ser::to_string(&message)?;
//                 write_half.write(raw_data.as_bytes()).await?;
//                 Ok(())
//             },
//         }
//     }
// }

// async fn read_handler<F, I: ChordId>(mut read_half: OwnedReadHalf, mut buffer: Vec<u8>, mut on_recv: F, sender: Sender<ChordOperation<I>>, last_msg: Arc<RwLock<DateTime<Utc>>>) -> JoinHandle<()>
// where for<'a> F: FnMut(ChordMessage<I>) -> Option<ChordOperation<I>> + 'a + std::marker::Send + 'static {
//     tokio::spawn(async move{
//         loop{
//             let msg = read_message(&mut read_half, &mut buffer).await;
//             match msg {
//                 Ok(msg) => {
//                     let mut lock = last_msg.write().await;
//                     *lock = Utc::now();
//                     drop(lock);
//                     match on_recv(msg){
//                         Some(op) => {
//                             sender.send(op).await;
//                         },
//                         None => {},
//                     }
//                 },
//                 Err(_) => todo!(),
//             }
//         }

//     })
// }

// async fn read_message<I: ChordId>(read_half: &mut OwnedReadHalf, mut buffer: &mut Vec<u8>) -> Result<ChordMessage<I>, std::io::Error>{
//     loop{

//         let mut des = Deserializer::from_slice(buffer.as_slice()).into_iter();

//         for res in &mut des{

//             match res{
//                 Ok(raw) => {
//                     let raw: RawMessage<I> = raw;
//                     match serde_json::from_slice(&raw.payload){
//                         Ok(chord_msg) => {
//                             let chord_msg: ChordMessage<I> = chord_msg;
                           
//                             buffer = &mut buffer[des.byte_offset()..].to_vec();
//                             return Ok(chord_msg);
//                         },
//                         Err(e) => {
//                             // Invalid message
//                         },
//                     }
//                 },
//                 Err(e) => {
//                     eprintln!("Encountered deserialization error: {}", e);
//                 },
//             }

//         }
        



//         let mut tmp_buf = vec![0; 1024];
//         match read_half.read(&mut tmp_buf).await {
//             Ok(0) => { 
//                 return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)); // No more data
//             }, 
//             Ok(len) => { // Append data to buffer
//                 buffer.extend_from_slice(&tmp_buf[..len]);
//             },
//             Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
//                 continue; // try to read again
//             },
//             Err(e) =>{
//                 println!("Encountered error reading from connection: {}", e);
//                 // probably should terminate connection here, depending on error
//                 continue;
//             }
//         }
//     }
// }