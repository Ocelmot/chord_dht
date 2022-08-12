#![deny(missing_docs)]

//! An implementation of the Chord dynamic hash table algorithm
//!
//! The implementation is generic over a given id type, address type, and
//! adaptor implementation to provide outgoing and incoming connections
//! to the chord itself.
//! 
//! # Examples
//! Create a new TCPChord node and join an existing chord.
//! ```
//!     // create a new chord that listens on localhost port 3001, with id 82.
//!     let chord = TCPChord::new(String::from("127.0.0.1:3001"), 82); 
//!     // start the node, and attempt to join an existing chord at localhost port 3000.
//!     let handle = chord.start(Some(String::from("127.0.0.1:3000"))).await;
//!     
//! 
//!     // get a new associate connection from the chord handle.
//!     let mut associate = chord.get_associate().await;
//! 
//!     // query the node for the successor of a particular key
//!     // let successor = 
//! 
//! ```
//! 
//! To create a new chord entirely, pass None as the join address to the start method.
//! ``` 
//!     # let chord = TCPChord::new(String::from("127.0.0.1:3001"), 82); 
//!     // start the node, but do not connect to an existing node to create a new chord
//!     let handle = chord.start(None).await;
//!     
//! ```


/// Contains the chord struct as well as supporting items.
/// 
/// 
pub mod chord;
use chord::Chord;

/// Provides the adaptor that converts addresses to connections,
/// and listens for new connections
pub mod adaptor;
pub use adaptor::tcp_adaptor::TCPAdaptor;


mod chord_id;
pub use chord_id::ChordId;



/// A Chord that is generic over a given id type and address type, but uses the TCPAdaptor implementation
pub type TCPChord<A, I> = Chord<A, I, TCPAdaptor<A, I>>;


/// Associate connections can query the chord but do not participate in it directly
pub mod associate;



use std::fmt::Debug;
use serde::{Serialize, Deserialize};

/// Represents the constraints that a ChordAddress must satisfy.
pub trait ChordAddress: Clone + Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de>{}
impl<T: Clone + Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de>> ChordAddress for T{}


