
use std::fmt::Debug;
use serde::{Serialize, Deserialize};


pub mod chord;
use chord::Chord;

pub mod adaptor;
pub use adaptor::tcp_adaptor::TCPAdaptor;


mod chord_id;
use chord_id::ChordId;




pub type TCPChord<A, I> = Chord<A, I, TCPAdaptor<A, I>>;



pub mod associate;






pub trait ChordAddress: Clone + Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de>{}
impl<T: Clone + Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de>> ChordAddress for T{}
