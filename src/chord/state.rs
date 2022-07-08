

use std::path::Path;

use serde::{Serialize, Deserialize};
use tokio::fs;

use crate::{ChordAddress, chord_id::ChordId};




#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct ChordState<A: ChordAddress, I: ChordId>{
    pub node_id: I,
    pub node_addr: A,
    pub listen_addr: Option<A>,

    pub known_addrs: Vec<A>
}


impl<A: ChordAddress, I: ChordId> ChordState<A, I> {
    pub async fn new(node_id: I, node_addr: A) -> Self {
        ChordState {
            node_id,
            node_addr,
            listen_addr: None,
            known_addrs: Vec::new(),
        }
    }

    pub async fn from_file<P: AsRef<Path>>(path: P) -> Self{
        let state = fs::read_to_string(&path).await.expect("Failed to read chord state from file");
		let chord_state = serde_json::from_str(&state).expect("Failed to deserialize chord state");
        chord_state
    }

    pub async fn save<P: AsRef<Path>>(&mut self, path: P){
        let s = serde_json::to_string(self).expect("Failed to serialize chord state");
        fs::write(path, s).await;
    }

}



