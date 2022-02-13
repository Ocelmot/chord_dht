use serde::{Serialize, Deserialize};
use tokio::net::{tcp::OwnedWriteHalf};

use crate::circular_id::CircularId;



#[derive(Serialize, Deserialize, Debug)]
pub struct RawMessage{
    pub to: CircularId,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum ProcessMessage{
    Control(ControlMessage),
    Message(ChordMessage),
    Broadcast(Vec<u8>),
}

#[derive(Debug)]
pub enum ControlMessage{
    IncomingConnection(OwnedWriteHalf, CircularId),

    Stabilize,
    Notify,
    FixFingers,
    CheckPredecessor
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChordMessage{
    pub from: CircularId,
    pub channel: u64,
    pub msg_type: ChordMessageType,

}

#[derive(Serialize, Deserialize, Debug)]
pub enum ChordMessageType{
    Lookup(CircularId),
    LookupReply(CircularId, Option<Vec<u8>>)
}

impl ChordMessage{
    pub fn into_raw(self, to: CircularId) -> Result<RawMessage, serde_json::Error>{
        let payload = serde_json::ser::to_vec(&self)?;
        Ok(RawMessage{
            to,
            payload,
        })
    }
}





pub struct ExternMessage{


}