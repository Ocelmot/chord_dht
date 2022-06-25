use std::time::Duration;
use std::{collections::BTreeMap};

mod ops;
mod stabilize;


use crate::{ chord_id::ChordId};


use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::task::JoinHandle;
use tokio::time::interval;


#[derive(Debug)]
pub enum ChordOperation<I: ChordId>{
    RegisterConnection{conn: Sender<ChordOperationResult<I>>},

    GetID,


    Ping(I),

    Stabilize,
    GetPredecessor{from: I},
    UpdateSuccessor{node: I, pred: I},
    Notify,
    Notified(I),
    FixFingers,
    CheckPredecessor,
    Cleanup,
}

#[derive(Debug)]
pub enum ChordOperationResult<I: ChordId>{
    ID(I)
}

#[derive(Debug, Clone)]
pub enum ProcessorId<M>{
    Member(M),
    Associate(u32),
}

pub(crate) struct ChordProcessor<I: ChordId>{
    // Core data
    self_id: I,
    predecessor: Option<I>,
    finger_table: Vec<I>,

    // Connections
    members: BTreeMap<I, Sender<ChordOperationResult<I>>>,
    associates: BTreeMap<u32, Sender<ChordOperationResult<I>>>,
    
    // Operations channel
    channel_rx: Receiver<(ProcessorId<I>, ChordOperation<I>)>,
    channel_tx: Sender<(ProcessorId<I>, ChordOperation<I>)>,

    // Task Handles
    stabilizer_handle: Option<JoinHandle<Result<(), SendError<ChordOperation<I>>>>>,
}

impl<I: ChordId> ChordProcessor<I>{
    pub fn new (self_id: I) -> Self{
        let (channel_tx, channel_rx) = channel::<(ProcessorId<I>, ChordOperation<I>)>(50);
        let connections = BTreeMap::new();
        ChordProcessor{
            // Core data
            self_id,
            predecessor: None,
            finger_table: Vec::new(),

            members: connections,
            associates: BTreeMap::new(),

            // Operations channel
            channel_rx,
            channel_tx,

            // Task Handles
            stabilizer_handle: None,
        }
    }

    pub fn get_channel(&self) -> Sender<(ProcessorId<I>, ChordOperation<I>)>{
        self.channel_tx.clone()
    }

    pub async fn start(mut self) -> JoinHandle<()>{

        // Start maintenance task
        let stabilizer_channel = self.channel_tx.clone();
        let stabilizer_handle = tokio::spawn(async move{
            let mut interval = interval(Duration::from_secs(15));
            loop{
                interval.tick().await;
                // stabilizer_channel.send((ProcessorId::Associate(0), ChordOperation::Stabilize)).await;
                // stabilizer_channel.send(ChordOperation::Notify).await?;
                // stabilizer_channel.send(ChordOperation::FixFingers).await?;
                // stabilizer_channel.send(ChordOperation::CheckPredecessor).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), SendError<ChordOperation<I>>>(())
        });
        self.stabilizer_handle = Some(stabilizer_handle);

        // Spawn operation task
        tokio::spawn(async move{
            while let Some((id, operation)) = self.channel_rx.recv().await{
                self.process_operation(id, operation).await;
            }
        })
    }

    async fn process_operation(&mut self, id: ProcessorId<I>, operation: ChordOperation<I>){
        match operation{
            ChordOperation::RegisterConnection { conn }=>{
                self.register_connection(id, conn).await;
            },
            ChordOperation::GetID => {
                self.send_result(id, ChordOperationResult::ID(self.self_id.clone())).await;
            },


            /////// Conversion point


            ChordOperation::Ping(_) => {}, //self.ping().await,
            //

            ChordOperation::Stabilize => {
                self.stabilize().await;
            },
            ChordOperation::Notify => {
                self.notify().await;
            },
            ChordOperation::Notified(notified_id) => {
                self.notified(notified_id).await;               
            },
            ChordOperation::FixFingers => {
                self.fix_fingers().await;
            },
            ChordOperation::CheckPredecessor => {
                self.check_predecessor().await;
            },
            ChordOperation::Cleanup => {
                // self.cleanup().await;
            },
            ChordOperation::GetPredecessor { from } => {
                self.get_predecessor(&from).await;
            },
            ChordOperation::UpdateSuccessor { node, pred } => {
                self.update_successor(node, pred).await;
            },
            
        };

    }



    async fn send_result(&mut self, id: ProcessorId<I>, result: ChordOperationResult<I>){
        match id{
            ProcessorId::Member(id) => {
                match self.members.get_mut(&id){
                    Some(channel) => {
                        channel.send(result).await;
                    },
                    None => {},
                }
            },
            ProcessorId::Associate(id) => {
                match self.associates.get_mut(&id){
                    Some(channel) => {
                        channel.send(result).await;
                    },
                    None => {},
                }
            },
        }
    }

}