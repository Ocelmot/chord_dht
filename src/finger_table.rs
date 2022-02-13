
use crate::CircularId;

use tokio::net::tcp::OwnedWriteHalf;

pub struct FingerTable<const BITS: u32>{
    fingers: Vec<(CircularId<BITS>, OwnedWriteHalf)>
}



