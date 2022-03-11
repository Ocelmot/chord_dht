use serde::{Serialize, Deserialize};

use crate::{circular_id::CircularId, node::Node};


#[derive(Serialize, Deserialize, Debug)]
pub struct RawMessage{
    pub to: CircularId,
    pub payload: Vec<u8>,
}


#[derive(Serialize, Deserialize, Debug)]
pub enum ChordMessage{
    Introduction{from: CircularId},
    Data{from: CircularId, data: Vec<i8>},
    Broadcast{id: u32, msg: String},

    GetPredecessor{from: CircularId},
    Predecessor{of: CircularId, is: CircularId},
    Notify(CircularId),
    Ping{from: CircularId},
    Pong,
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


#[derive(Debug)]
pub enum ChordOperation{
    IncomingConnection(Node),
    Message(ChordMessage),
    Forward(RawMessage),
    Broadcast(Option<u32>, String),
    Ping(CircularId),

    Stabilize,
    Predecessor(CircularId, CircularId),
    Notify,
    Notified(CircularId),
    FixFingers,
    CheckPredecessor,
    Cleanup,
}
