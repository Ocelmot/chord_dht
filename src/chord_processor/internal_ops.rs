use tokio::sync::mpsc::Sender;

use crate::{ChordProcessor, chord_id::ChordId, adaptor::ChordAdaptor};

use super::{ProcessorId, ChordOperationResult};




impl<A, I: ChordId, ADAPTOR: ChordAdaptor<A, I>> ChordProcessor<A, I, ADAPTOR>{
    pub(crate) async fn register_connection(&mut self, id: ProcessorId<I>, conn: Sender<ChordOperationResult<A, I>>){
        match id {
            ProcessorId::Member(id) => {
                self.members.insert(id, conn);
            },
            ProcessorId::Associate(id) => {
                self.associates.insert(id, conn);
            },
        };
    }

}