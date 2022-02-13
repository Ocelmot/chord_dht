

use std::io::ErrorKind;

use serde_json::Deserializer;
use tokio::{net::{TcpStream, tcp::OwnedWriteHalf}, io::{AsyncWriteExt, AsyncReadExt}, task::JoinHandle};

use crate::{circular_id::CircularId, chord_msg::{ChordMessage, RawMessage}};

pub struct Node{
    id: CircularId,
    write_half: OwnedWriteHalf,
    read_handle: JoinHandle<()>,
}

impl Node{

    fn new(id: CircularId, sock: TcpStream, on_recv:fn(ChordMessage)) -> Node{
        let (mut read_half, write_half) = sock.into_split();
        let read_handle =  tokio::spawn(async move{
            let mut buffer = Vec::new();
            loop{

                let mut tmp_buf = vec![0; 1024];
                match read_half.read(&mut tmp_buf).await {
                    Ok(0) => { 
                        return; // No more data
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

                let mut des = Deserializer::from_slice(buffer.as_slice()).into_iter();

                for res in &mut des{

                    match res{
                        Ok(raw) => {
                            let raw: RawMessage = raw;
                            match serde_json::from_slice(&raw.payload){
                                Ok(chord_msg) => {
                                    let chord_msg: ChordMessage = chord_msg;
                                   
                                    on_recv(chord_msg);
                                    
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
                buffer = buffer[des.byte_offset()..].to_vec();

            }
        });

        Node{
            id,
            write_half,
            read_handle,
        }
    }

    async fn send(&mut self, message: ChordMessage) -> Result<(), std::io::Error>{
        let raw_msg = message.into_raw(self.id.clone())?;
        let raw_data = serde_json::ser::to_string(&raw_msg)?;
        self.write_half.write(raw_data.as_bytes()).await?;
        Ok(())
    }


}


