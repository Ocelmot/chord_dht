use std::{io::ErrorKind, collections::BTreeMap, time::Duration};


use tokio::{net::{TcpListener, TcpStream, ToSocketAddrs, tcp::{OwnedReadHalf, OwnedWriteHalf}}, time::Instant, io::AsyncWriteExt};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio::time::{interval};

use serde_json::{Deserializer};


pub mod circular_id;
use circular_id::CircularId;

pub mod node;
use node::Node;

pub mod chord_msg;
use chord_msg::{RawMessage, ProcessMessage, ChordMessage, ControlMessage, ChordMessageType, ExternMessage};


pub struct DHTChord{
    broadcast_ch: Sender<ProcessMessage>,
    pub process_handle: JoinHandle<()>
}

impl DHTChord{


    pub async fn new<A: ToSocketAddrs>(listen_addr: A, join_addr: Option<A>) ->DHTChord{
        let listener = TcpListener::bind(listen_addr).await;
        let listener = listener.expect("Could not create listen socket!");

        let (msg_tx, msg_rx) = channel::<ProcessMessage>(50);

        // Spawn task to listen and generate tasks to supply the channel
        // println!("Spawning listen task");
        tokio::spawn(listen_handler(listener, msg_tx.clone()));
        
        // spawn message processing task
        let ph = tokio::spawn(message_processor(msg_rx));

        // spawn maintenence task
        tokio::spawn(chord_upkeep(msg_tx.clone()));

        // setup way for outside to query results from chord, return reference to chord
        // Add way to stop queues gracefully
        DHTChord{
            broadcast_ch: msg_tx,
            process_handle: ph 
        }
    }

    pub async fn send_broadcast(&self, data:Vec<u8>){
        self.broadcast_ch.send(ProcessMessage::Broadcast(data));
    }

    pub async fn recv_broadcast(&mut self) -> Vec<u8>{
        todo!();
    }

    pub async fn send_message(self, data:Vec<u8>) -> Vec<u8>{
        // let (channel_tx, channel_rx) = channel(1);

        todo!()
    }
}

async fn listen_handler(listener: TcpListener, channel: Sender<ProcessMessage>){
    // println!("Listening!...");
    loop{
        match listener.accept().await{
            Err(e) => { 
                /* TODO: probably shut down listener */
                eprintln!("Encountered an error in accept: {}", e)
            },
            Ok((stream, _)) => {
                // Spawn sock handlers
                // println!("Spawning socket handler");
                tokio::spawn(sock_handler(stream, channel.clone()));
            }
        }
    }
}

async fn sock_handler(stream: TcpStream, channel: Sender<ProcessMessage> ){
    // add incoming messages into queue
    let stream_parts = stream.into_split();
    let stream_read = stream_parts.0;
    let mut stream_write = Some(stream_parts.1);

    let mut buffer = Vec::new();
    loop{
        stream_read.readable().await;
        let mut tmp_buf = vec![0; 1024];
        match stream_read.try_read(&mut tmp_buf) {
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
                            stream_write = if let Some(writer) = stream_write {
                                let to = chord_msg.from.clone();
                                
                                channel.send(ProcessMessage::Control(ControlMessage::IncomingConnection(writer, to))).await;
                                None
                            }else{None};
                            let proc_msg = ProcessMessage::Message(chord_msg);
                            channel.send(proc_msg).await;
                            
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

}

struct ProcessorData{
    self_id: CircularId,
    finger_table: Vec<(CircularId, OwnedWriteHalf)>,
    incoming_connections: BTreeMap<CircularId, OwnedWriteHalf>,
    datastore: BTreeMap<CircularId, Vec<u8>>,
}

async fn message_processor(mut channel: Receiver<ProcessMessage>){
    let mut data = ProcessorData{
        self_id: CircularId::rand(),
        finger_table: Vec::new(),
        incoming_connections: BTreeMap::new(),
        datastore: BTreeMap::new(),
    };
    while let Some(message) = channel.recv().await{
        // println!("Processing message...");
        // add forward code here
        match message{
            ProcessMessage::Control(cm) => {process_control(cm, &mut data).await;},
            ProcessMessage::Message(pm) =>{process_chord(pm, &mut data).await;},
            ProcessMessage::Broadcast(bm) => {},
        }
    }
}

async fn process_control(cm: ControlMessage, data: &mut ProcessorData){
    match cm{
        ControlMessage::IncomingConnection(writer, address) => {
            data.incoming_connections.insert(address, writer);
        },
        ControlMessage::Stabilize => {
            // ask successor for its pred. if that node is closer than current successor, update successor
            let succ = &data.finger_table[0];
        },
        ControlMessage::Notify => {
            // notify successor about us!
            let succ = &data.finger_table[0];
            // let sock = succ.1;
            // sock.write();

        },
        ControlMessage::FixFingers => {
            // for each finger, try to find the correct node to point to, then replace finger with that node.
        },
        ControlMessage::CheckPredecessor => {
            // Ping predecessor to check for liveness, if dead, set to none, else is fine
        },
        // _ =>{},
    }

}
async fn process_chord(cm: ChordMessage, data: &mut ProcessorData){
    match cm.msg_type{
        chord_msg::ChordMessageType::Lookup(ref lookup_id) => {
            if data.finger_table.len() == 0 || lookup_id.is_between(&data.self_id, &data.finger_table[0].0){
                let dat = data.datastore.get(lookup_id).cloned();
                let rm = ChordMessage{from: data.self_id.clone(), channel: cm.channel, msg_type: ChordMessageType::LookupReply(lookup_id.clone(), dat)};
                send_message(Some(&mut data.finger_table), Some(&mut data.incoming_connections), rm, cm.from).await;
            }else{ // forward request
                let dest = lookup_id.clone();
                send_message(Some(&mut data.finger_table), Some(&mut data.incoming_connections), cm, dest).await;
            }
        },
        chord_msg::ChordMessageType::LookupReply(_, _) => todo!(),
    };
}

async fn send_message(
    finger_table: Option<&mut Vec<(CircularId, OwnedWriteHalf)>>,
    extra_connections: Option<&mut BTreeMap<CircularId, OwnedWriteHalf>>,
    message: ChordMessage,
    to: CircularId,
) -> Result<(), std::io::Error>{

    if let Some(connections) = extra_connections{
        match connections.get_mut(&to){
            Some(connection) => {
                let raw_msg = message.into_raw(to).expect("failed to generate raw message");
                let raw_data = serde_json::ser::to_string(&raw_msg).expect("failed to serialize raw message");
                connection.write(raw_data.as_bytes()).await?;
                return Ok(());
            },
            None => {},
        }
    }


    if let Some(finger_table ) = finger_table{
        if finger_table.len() > 1{
            let (head, tail) = finger_table.split_at_mut(1);
            let head = &head[0];
            for i in (0..tail.len()).rev(){
                let node = &mut tail[i];
                if !to.is_between(&head.0, &node.0){ // not in remaining segments, send to node
                    let connection = &mut node.1;
                    let raw_msg = message.into_raw(to).expect("failed to generate raw message");
                    let raw_data = serde_json::ser::to_string(&raw_msg).expect("failed to serialize raw message");
                    connection.write(raw_data.as_bytes()).await?;
                    return Ok(());
                }
            }
            // return fail(should only occur if we would have to send to ourselves) todo
        }
        
    }

    Ok(())
}

async fn chord_upkeep(channel: Sender<ProcessMessage>) -> Result<(), SendError<ProcessMessage>>{
    let mut interval = interval(Duration::from_secs(15));
    loop{
        interval.tick().await;
        channel.send(ProcessMessage::Control(ControlMessage::Stabilize)).await?;
        channel.send(ProcessMessage::Control(ControlMessage::Notify)).await?;
        channel.send(ProcessMessage::Control(ControlMessage::FixFingers)).await?;
        channel.send(ProcessMessage::Control(ControlMessage::CheckPredecessor)).await?;
    }
}

#[cfg(test)]
mod tests {
    // use crate::{DHTChord};

    #[tokio::test]
    async fn run_it() {
        // let chord = DHTChord::new("localhost:1930", None).await;

        // println!("Created chord, entering loop!");
        
    }

    #[test]
    fn json_encoding(){
        // let val = ChordMessage::Lookup;
        // let str = serde_json::to_string(&val).expect("Serialization failure");
        // println!("encoding: {}", str);
    }
}
