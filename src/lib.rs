use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use adaptor::{ChordAdaptor, tcp_adaptor::TCPAdaptor};
use chord_processor::{ChordOperation, ChordOperationResult, ProcessorId};

use tokio::io;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::task::JoinHandle;


mod adaptor;

mod chord_id;
use chord_id::ChordId;

pub type TCPChord<A, I> = Chord<A, I, TCPAdaptor>;

pub mod chord_processor;
use crate::chord_processor::ChordProcessor;

pub struct Chord<A, I: ChordId, ADAPTOR: ChordAdaptor<A, I>>{
    processor_input: Sender<(ProcessorId<I>, ChordOperation<A, I>)>,
    processor_output: Receiver<ChordOperationResult<A, I>>,

    processor_handle: JoinHandle<()>,
    marker: PhantomData<A>,
    marker2: PhantomData<ADAPTOR>,
}

impl<A: Clone + Send + 'static, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> Chord<A, I, ADAPTOR>{


    pub async fn new(self_id: I, listen_addr: A, join_addr: Option<A>) -> io::Result<Chord<A, I, ADAPTOR>>{
        
        let (processor_output_tx, processor_output_rx) = channel::<ChordOperationResult<A, I>>(50);

        // Create and start the message processor
        let processor = ChordProcessor::<A, I, ADAPTOR>::new(listen_addr.clone(), self_id);
        let processor_input = processor.get_channel();
        let processor_handle = processor.start(join_addr).await;

        processor_input.send((chord_processor::ProcessorId::Associate(0), ChordOperation::RegisterConnection {conn: processor_output_tx })).await;



        Ok(Chord{
            processor_input,
            processor_output: processor_output_rx,

            processor_handle,

            marker: PhantomData,
            marker2: PhantomData,
        })
    }


    pub async fn operation(&mut self, op: ChordOperation<A, I>) -> Option<ChordOperationResult<A, I>>{
        self.processor_input.send((chord_processor::ProcessorId::Associate(0), op)).await;
        self.processor_output.recv().await
    }

    pub async fn send_op(&mut self, op: ChordOperation<A, I>){
        self.processor_input.send((chord_processor::ProcessorId::Associate(0), op)).await;
    }

    pub async fn recv_op(&mut self) -> Option<ChordOperationResult<A, I>>{
        self.processor_output.recv().await
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
