use std::{cmp::Ordering, fmt::Debug};
use serde::{Serialize, Deserialize};

/// A ChordId is a point around the chord ring. It must be able to be compared with other ChordId's, calculate
/// what the ChordId of a finger of given index should be, and determine the index after a given index.
/// A ChordId should also define the point at which it wraps back to the beginning of the sequence.
pub trait ChordId: Clone + Ord + Sync + Send + Serialize + for<'de> Deserialize<'de> + 'static + Debug{
	/// The maximum acceptable value of a ChordId.
	fn wrap_point() -> Self;
	/// Calculate the next index, given the previous index.
	/// This should wrap back to one after reaching the maximum index.
	fn next_index(prev_index: u32) -> u32;
	/// Calculate the ChordId of a finger, given this ChordId and an index.
	fn calculate_finger(&self, index :u32) -> Self;
		
	/// Tests if self is in the range (lower, upper]
	fn is_between(&self, lower: &Self, upper: &Self) -> bool {
		match lower.cmp(upper){
			Ordering::Less => (self > lower) && (self <= upper),
			Ordering::Equal => self == lower,
			Ordering::Greater => (self > lower) || (self <= upper),
		}
	}

}


impl ChordId for u32{
	fn wrap_point() -> Self {
		u32::max_value()
	}
	fn next_index(prev_index: u32) -> u32 {
		let mut next_index = prev_index + 1;
		if next_index > 32 {
			next_index = 1;
		}
		next_index
	}
	fn calculate_finger(&self, index :u32) -> Self {
		let offset = 2u32.pow(index - 1);
		self.wrapping_add(offset)
	}
}
