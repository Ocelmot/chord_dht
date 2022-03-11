

use std::{io::ErrorKind, sync::Arc};
use chrono::{DateTime, Utc};

use serde_json::Deserializer;
use tokio::{net::{TcpStream, tcp::{OwnedWriteHalf, OwnedReadHalf}, ToSocketAddrs}, io::{AsyncWriteExt, AsyncReadExt}, task::JoinHandle, sync::{RwLock, mpsc::Sender}};

use crate::{circular_id::CircularId, chord_msg::{ChordMessage, RawMessage, ChordOperation}};

#[derive(Debug)]
pub enum Node{
    Local{id: CircularId},
    Other{
        id: CircularId,
        write_half: OwnedWriteHalf,
        read_handle: JoinHandle<()>,
        last_msg: Arc<RwLock<DateTime<Utc>>>,
    }
}

impl Node{

    pub async fn connect<A, F: 'static>(id: CircularId, addr: A, sender: Sender<ChordOperation>, on_recv: F) -> Result<Node, std::io::Error>
    where A: ToSocketAddrs,
        F: FnMut(ChordMessage) -> Option<ChordOperation> + std::marker::Send{

        let stream = TcpStream::connect(addr).await?;

        let (read_half, write_half) = stream.into_split();

        let last_msg = Arc::new(RwLock::new(Utc::now()));

        let read_handle = read_handler(read_half, Vec::new(), on_recv, sender, last_msg.clone()).await; 


        let mut res = Node::Other{
            id: id.clone(),
            write_half,
            read_handle,
            last_msg,
        };
        res.send(ChordMessage::Introduction{from: id});
        Ok(res)
    }

    pub async fn from_stream<F: 'static>(stream: TcpStream, sender: Sender<ChordOperation>, on_recv: F) -> Result<Node, std::io::Error>
    where F: FnMut(ChordMessage) -> Option<ChordOperation> + std::marker::Send{
        // listen over socket until the stream has introduced itself
        // then use that id to init the Node
        let (mut read_half, write_half) = stream.into_split();

        let mut buffer = Vec::new();
        
        let intro = read_message(&mut read_half, &mut buffer).await?;
        
        let id = if let ChordMessage::Introduction{from} = intro{
            from
        } else {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        };

        let last_msg = Arc::new(RwLock::new(Utc::now()));

        let read_handle =  read_handler(read_half, buffer, on_recv, sender, last_msg.clone()).await;


        Ok(Node::Other{id, write_half, read_handle, last_msg})
    }

    pub async fn send(&mut self, message: ChordMessage) -> Result<(), std::io::Error>{
        match self{
            Node::Local {..} => todo!(),
            Node::Other { id, write_half, read_handle, last_msg } => {
                let raw_msg = message.into_raw(id.clone())?;
                self.send_raw(raw_msg).await
            },
        }
    }

    pub async fn send_raw(&mut self, message: RawMessage) -> Result<(), std::io::Error>{
        match self{
            Node::Local{..} => todo!(),
            Node::Other { id, write_half, read_handle, last_msg } => {
                
                let raw_data = serde_json::ser::to_string(&message)?;
                write_half.write(raw_data.as_bytes()).await?;
                Ok(())
            },
        }
    }
}

// trait 

async fn read_handler<F>(mut read_half: OwnedReadHalf, mut buffer: Vec<u8>, mut on_recv: F, sender: Sender<ChordOperation>, last_msg: Arc<RwLock<DateTime<Utc>>>) -> JoinHandle<()>
where for<'a> F: FnMut(ChordMessage) -> Option<ChordOperation> + 'a + std::marker::Send + 'static {
    tokio::spawn(async move{
        loop{
            let msg = read_message(&mut read_half, &mut buffer).await;
            match msg {
                Ok(msg) => {
                    let mut lock = last_msg.write().await;
                    *lock = Utc::now();
                    drop(lock);
                    match on_recv(msg){
                        Some(op) => {
                            sender.send(op).await;
                        },
                        None => {},
                    }
                },
                Err(_) => todo!(),
            }
        }

    })
}

async fn read_message(read_half: &mut OwnedReadHalf, mut buffer: &mut Vec<u8>) -> Result<ChordMessage, std::io::Error>{
    loop{

        let mut des = Deserializer::from_slice(buffer.as_slice()).into_iter();

        for res in &mut des{

            match res{
                Ok(raw) => {
                    let raw: RawMessage = raw;
                    match serde_json::from_slice(&raw.payload){
                        Ok(chord_msg) => {
                            let chord_msg: ChordMessage = chord_msg;
                           
                            buffer = &mut buffer[des.byte_offset()..].to_vec();
                            return Ok(chord_msg);
                        },
                        Err(e) => {
                            // Invalid message
                        },
                    }
                },
                Err(e) => {
                    eprintln!("Encountered deserialization error: {}", e);
                },
            }

        }
        



        let mut tmp_buf = vec![0; 1024];
        match read_half.read(&mut tmp_buf).await {
            Ok(0) => { 
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)); // No more data
            }, 
            Ok(len) => { // Append data to buffer
                buffer.extend_from_slice(&tmp_buf[..len]);
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