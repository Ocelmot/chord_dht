use std::{collections::BTreeMap, ops::Bound::{Included, Unbounded}, time::Duration};


use tokio::{net::{TcpListener, ToSocketAddrs}, io::{self}};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::mpsc::error::SendError;
use tokio::task::JoinHandle;
use tokio::time::interval;


pub mod circular_id;
use circular_id::CircularId;

pub mod node;
use node::Node;

pub mod chord_msg;
use chord_msg::{ChordMessage, ChordOperation, RawMessage};

use chrono::Utc;

mod operations;

pub struct DHTChord{
    channel: Sender<ChordOperation>,
    broadcast_channel_rx: Receiver<String>,

    processor_handle: JoinHandle<()>,
    listener_handle: JoinHandle<()>,
    stabilizer_handle: JoinHandle<Result<(), SendError<ChordOperation>>>,
}

impl DHTChord{


    pub async fn new<A: ToSocketAddrs>(listen_addr: A, join_addr: Option<A>) -> io::Result<DHTChord>{
        
        let (broadcast_channel_tx, broadcast_channel_rx) = channel::<String>(50);

        // Create and start the message processor
        let processor = ProcessorData::new(CircularId::rand(), broadcast_channel_tx);
        let processor_channel = processor.get_channel();
        let processor_handle = processor.start().await;




        let listener = TcpListener::bind(listen_addr).await?;
        // let listener = listener.expect("Could not create listen socket!");

        // Spawn task to listen and generate tasks to supply the channel
        let listener_handle = tokio::spawn(listen_handler(listener, processor_channel.clone()));




        let stabilizer_channel = processor_channel.clone();
        let stabilizer_handle = tokio::spawn(async move{
            let mut interval = interval(Duration::from_secs(15));
            loop{
                interval.tick().await;
                stabilizer_channel.send(ChordOperation::Stabilize).await?;
                stabilizer_channel.send(ChordOperation::Notify).await?;
                stabilizer_channel.send(ChordOperation::FixFingers).await?;
                stabilizer_channel.send(ChordOperation::CheckPredecessor).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), SendError<ChordOperation>>(())
        });

        

        // setup way for outside to query results from chord, return reference to chord
        // Add way to stop queues gracefully
        Ok(DHTChord{
            channel: processor_channel,
            broadcast_channel_rx,

            processor_handle,
            listener_handle,
            stabilizer_handle,
        })
    }

    pub async fn send_broadcast(&mut self, data: String){
        match self.channel.send(ChordOperation::Broadcast(None, data)).await{
            Ok(_) => {},
            Err(e) => {
                panic!("Error: {}", e);
            },
        }
    }

    pub async fn recv_broadcast(&mut self) -> Option<String>{
        self.broadcast_channel_rx.recv().await
    }

    // pub async fn send_message(self, data:Vec<u8>) -> Vec<u8>{
    //     // let (channel_tx, channel_rx) = channel(1);

    //     todo!()
    // }
}

async fn listen_handler(listener: TcpListener, channel: Sender<ChordOperation>){
    // println!("Listening!...");
    loop{
        match listener.accept().await{
            Err(e) => { 
                /* TODO: probably shut down listener */
                eprintln!("Encountered an error in accept: {}", e)
            },
            Ok((stream, _)) => {
                // Spawn sock handlers
                let ch = channel.clone();
                let node = Node::from_stream(stream, ch, move |msg|{
                    match msg {
                        ChordMessage::Introduction { from } => None,
                        ChordMessage::Data { from, data } => todo!(),
                        ChordMessage::Broadcast { id, msg } => Some(ChordOperation::Broadcast(Some(id), msg)),

                        ChordMessage::GetPredecessor {from} => todo!(),
                        ChordMessage::Predecessor { of, is } => todo!(),
                        // respond to notify operation
                        ChordMessage::Notify(id) => Some(ChordOperation::Notified(id)),
                        ChordMessage::Ping { from } => Some(ChordOperation::Ping(from.clone())),
                        ChordMessage::Pong => None,
                    }
                }).await;

                match node{
                    Ok(node) => { // Got node, send to processor
                        channel.send(ChordOperation::IncomingConnection(node)).await;
                    },
                    Err(_) => {}, // Connection failed, drop
                };
                
            }
        }
    }
}


struct ProcessorData{
    // Core data
    connections: BTreeMap<CircularId, Node>,
    self_id: CircularId,
    predecessor: Option<CircularId>,
    finger_table: Vec<CircularId>,
    
    

    // Operations channel
    channel_rx: Receiver<ChordOperation>,
    channel_tx: Sender<ChordOperation>,

    // Broadcast items
    broadcast_ids: Vec<u32>,
    broadcast_channel_tx: Sender<String>,

    // Data storage
    datastore: BTreeMap<CircularId, Vec<u8>>,
}

impl ProcessorData{
    fn new (self_id: CircularId, broadcast_channel_tx: Sender<String>) -> ProcessorData{
        let (channel_tx, channel_rx) = channel::<ChordOperation>(50);
        let mut connections = BTreeMap::new();
        connections.insert(self_id.clone(), Node::Local{id:self_id.clone()});
        ProcessorData{
            connections,
            self_id,
            predecessor: None,
            finger_table: Vec::new(),
            
            
            broadcast_ids: Vec::new(),
            broadcast_channel_tx,

            channel_rx,
            channel_tx,

            datastore: BTreeMap::new(),
        }
    }

    async fn start(mut self) -> JoinHandle<()>{
        // println!("Starting processor");
        
        tokio::spawn(async move{
            // println!("processing");
            while let Some(operation) = self.channel_rx.recv().await{
                // println!("Looping");
                self.process_operation(operation).await;
            }
            // eprintln!("Exited processor loop");
        })
    }

    fn get_channel(&self) -> Sender<ChordOperation>{
        self.channel_tx.clone()
    }



    async fn process_operation(&mut self, operation: ChordOperation){
        match operation{
            ChordOperation::IncomingConnection(node) => {
                operations::incoming_connection(self, node);
                // match node{
                //     Node::Local { .. } => {},
                //     Node::Other { ref id, ..} => {
                //         self.connections.insert(id.clone(), node);
                //     },
                // }
                
            },
            ChordOperation::Message(msg) => {self.process_message(msg).await;},
            ChordOperation::Forward(msg) => {self.send_raw(msg).await;},
            ChordOperation::Broadcast(id, s) => {
                let (should_process, id) = match id{
                    Some(id) => { // have an id, we are forwarding/recving
                        // need to check if we have already recieved this id
                        let ret = !self.broadcast_ids.contains(&id);
                        if ret { // if we have not recvd this id, send to user
                            self.broadcast_channel_tx.send(s.clone());
                        }
                        (ret, id)
                    },
                    None => { // no id, need to create an id first
                        println!("broadcasting {}", s.clone());
                        (true, rand::random())
                    },
                };
                if should_process {
                    self.broadcast_ids.push(id); // mark as recvd
                    // forward the string
                    for (_, node) in &mut self.connections{
                        match node {
                            Node::Local { .. } => {},
                            Node::Other { .. } => {
                                node.send(ChordMessage::Broadcast{id, msg:s.clone()}).await;
                            },
                        }
                    }

                }
            },
            ChordOperation::Ping(_) => todo!(),
            //

            ChordOperation::Stabilize => {
                // ask successor for its pred. if that node is closer than current successor, update successor
                if let Some(successor_id) = self.finger_table.get(0){
                    if let Some(successor) = self.connections.get(successor_id){
                    //    successor.send(message) 
                    }
                }
            },
            ChordOperation::Notify => {
                // notify successor about us!
                if let Some(successor_id) = self.finger_table.get(0){
                    if let Some(node) = self.connections.get_mut(successor_id){
                        node.send(ChordMessage::Notify(self.self_id.clone())).await;
                    }
                }
            },
            ChordOperation::Notified(notified_id) => {
                // if we are notified of a new predecessor, check against our curent predecessor id
                // and update if needed
                match &self.predecessor{
                    Some(predecessor) => {
                        if notified_id.is_between(&predecessor, &self.self_id){
                            self.predecessor = Some(notified_id);
                        }
                        // otherwise ignore
                    },
                    None => {
                        self.predecessor = Some(notified_id);
                    },
                }
                
            },
            ChordOperation::FixFingers => {
                // for each finger, try to find the correct node to point to, then replace finger with that node.
            },
            ChordOperation::CheckPredecessor => {
                // Ping predecessor to check for liveness, if dead, set to none, else is fine
                if let Some(predecessor) = &self.predecessor { // if we have a predecessor
                    if let Some(node) = self.connections.get_mut(predecessor) {
                        if let Node::Other{ last_msg , .. } = node{
                            let x = last_msg.read().await;
                            if x.cmp(&(Utc::now() - chrono::Duration::seconds(30) )).is_le() {
                                // TODO: send ping
                                drop(x);
                                node.send(ChordMessage::Ping{from: self.self_id.clone()}).await;
                            }
                        }
                    }
                }
            },
            ChordOperation::Cleanup => {
                let mut to_remove = Vec::new();

                for (id, node) in &self.connections{
                    if let Node::Other{ last_msg, .. } = node{
                        let x = last_msg.read().await;
                        if x.cmp(&(Utc::now() - chrono::Duration::seconds(60) )).is_le() {
                            // TODO: close node
                            to_remove.push(id.clone());
                            // node.close();
                        }
                    }
                }
                for id in to_remove{
                    if let Some(pred) = &self.predecessor{
                        if pred.eq(&id){
                            self.predecessor = None;
                        }
                    }
                    self.connections.remove(&id);
                }
            },
            ChordOperation::Predecessor(_, _) => todo!(),
            
            
            
            // ChordOperation::Control(cm) => {process_control(cm, &mut data).await;},
            // ProcessMessage::Message(pm) =>{process_chord(pm, &mut data).await;},
            // ProcessMessage::Broadcast(bm) => {},
        };
    }

    async fn process_message(&mut self, msg: ChordMessage){


    }


    async fn send_message(&mut self, msg: ChordMessage, to: CircularId) -> Result<(), std::io::Error>{
        let range = (Unbounded, Included(&to));
        let node = self.connections.range_mut(range).next_back();
        let node = match node{
            Some((id, node)) => node,
            None => {
                self.connections.range_mut(..).next_back().unwrap().1
            },
        };

        node.send(msg);

        Ok(())
    }

    async fn send_raw(&mut self, msg: RawMessage) -> Result<(), std::io::Error>{
        let range = (Unbounded, Included(&msg.to));
        let node = self.connections.range_mut(range).next_back();
        let node = match node{
            Some((_id, node)) => node,
            None => {
                self.connections.range_mut(..).next_back().unwrap().1
            },
        };

        node.send_raw(msg);

        Ok(())
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
