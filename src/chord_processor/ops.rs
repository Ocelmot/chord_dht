use tokio::sync::mpsc::Sender;

use crate::{ChordProcessor, chord_id::ChordId};

use super::{ProcessorId, ChordOperationResult};




impl<I: ChordId> ChordProcessor<I>{
    pub(crate) async fn register_connection(&mut self, id: ProcessorId<I>, conn: Sender<ChordOperationResult<I>>){
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