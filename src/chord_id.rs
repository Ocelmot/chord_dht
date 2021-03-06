use std::{cmp::Ordering, fmt::Debug};
use serde::{Serialize, Deserialize};

pub trait ChordId: Clone + Ord + Sync + Send + Serialize + for<'de> Deserialize<'de> + 'static + Debug{
	fn wrap_point() -> Self;
	fn next_index(prev_index: u32) -> u32;
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














///////////////////Implementation of old CircularID, USE FOR IMPLEMENTING ChordID for bigints!!!





// use std::fmt;
// use std::cmp::Ordering;

// use num_bigint::{BigUint, RandBigInt};

// use serde::{
//     ser::{Serialize, Serializer, SerializeSeq},
//     de::{Deserialize, Deserializer, SeqAccess, Visitor},
// };


// #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
// pub struct CircularId{
//     id: BigUint,
// }

// const BITS: u32 = 32;

// impl CircularId{
//     pub fn new(data: &str)->Option<CircularId>{
//         let id = BigUint::parse_bytes(data.as_bytes(), 10);
//         let id = match id {
//             Some(id) =>{
//                 id % BigUint::new(vec!(2)).pow(BITS)
//             }
//             None => return None
//         };
//         Some(CircularId{id})
//     }
//     pub fn from_vec(vec: Vec<u32>)-> CircularId{
//         let mut id = BigUint::new(vec);
//         id = id % BigUint::new(vec!(2)).pow(BITS);
//         CircularId{id}
//     }
//     pub fn zero()->CircularId{
//         CircularId{id:BigUint::new(vec!(0))}
//     }
//     pub fn rand()->CircularId{
//         let mut rng = rand::thread_rng();  
//         let id = rng.gen_biguint_below(&(BigUint::new(vec!(2)).pow(BITS)));
//         CircularId{id}
//     }

//     /// index 0 = very next address
//     pub fn calculate_finger(&self, index :u32) -> CircularId{
//         let mut id = self.id.clone();
//         id = id + BigUint::from(2u32).pow(index);

//         id = id % BigUint::new(vec!(2)).pow(BITS);
//         CircularId{id}
//     }

//     /// Tests if self is in the range [lower, upper)
//     pub fn is_between(&self, lower: &CircularId, upper: &CircularId) -> bool {
//         match lower.cmp(upper){
//             Ordering::Less => (self.id >= lower.id) && (self.id < upper.id),
//             Ordering::Equal => self.id == lower.id,
//             Ordering::Greater => (self.id >= lower.id) || (self.id < upper.id),
//         }
//     }
// }

// impl Clone for CircularId{
//     fn clone(&self) -> CircularId{
//         CircularId::from_vec(self.id.to_u32_digits().clone())
//     }
// }

// impl From<u32> for CircularId{
//     fn from(arg: u32) -> CircularId{
//         CircularId::from_vec(vec!(arg))
//     }
// }

// impl Serialize for CircularId {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         let v = self.id.to_u32_digits();
//         let mut seq = serializer.serialize_seq(Some(v.len()))?;
//         for e in v {
//             seq.serialize_element(&e)?;
//         }
//         seq.end()
//     }
// }


// struct CircularIdVisitor;

// impl<'de, > Visitor<'de> for CircularIdVisitor {
//     type Value = CircularId;

//     fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//         formatter.write_str("Circular Id from an array of u32")
//     }

//     fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
//         where
//             S: SeqAccess<'de>
//         {
//             //let size = seq.size_hint().ok_or(serde::de::Error::custom("Deserialization requires size"))?;
//             let mut v = Vec::new();
//             loop{
//                 match seq.next_element(){
//                     Ok(result)=>{
//                         match result{
//                             Some(element) => {
//                                 v.push(element)
//                             },
//                             None => {
//                                 return Ok(CircularId::from_vec(v))
//                             },
//                         }
//                     },
//                     Err(e) => {
//                         return Err(e);
//                     }
//                 }
//             }
//     }
// }

// impl<'de> Deserialize<'de> for CircularId {
//     fn deserialize<D>(deserializer: D) -> Result<CircularId, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         deserializer.deserialize_seq(CircularIdVisitor)
//     }
// }