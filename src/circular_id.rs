use std::fmt;
use std::cmp::Ordering;

use num_bigint::{BigUint, RandBigInt};

use serde::{
    ser::{Serialize, Serializer, SerializeSeq},
    de::{Deserialize, Deserializer, SeqAccess, Visitor},
};


#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct CircularId{
    id: BigUint,
}

const BITS: u32 = 32;

impl CircularId{
    pub fn new(data: &str)->Option<CircularId>{
        let id = BigUint::parse_bytes(data.as_bytes(), 10);
        let id = match id {
            Some(id) =>{
                id % BigUint::new(vec!(2)).pow(BITS)
            }
            None => return None
        };
        Some(CircularId{
            id: id
        })
    }
    pub fn from_vec(vec: Vec<u32>)-> CircularId{
        let mut id = BigUint::new(vec);
        id = id % BigUint::new(vec!(2)).pow(BITS);
        CircularId{id}
    }
    pub fn zero()->CircularId{
        CircularId{id:BigUint::new(vec!(0))}
    }
    pub fn rand()->CircularId{
        let mut rng = rand::thread_rng();  
        let id = rng.gen_biguint_below(&(BigUint::new(vec!(2)).pow(BITS)));
        CircularId{id}
    }

    pub fn is_between(&self, lower: &CircularId, upper: &CircularId) -> bool {
        match lower.cmp(upper){
            Ordering::Less => (self.id > lower.id) && (self.id < upper.id),
            Ordering::Equal => self.id == lower.id,
            Ordering::Greater => (self.id > lower.id) || (self.id < upper.id),
        }
    }
}

impl Clone for CircularId{
    fn clone(&self) -> CircularId{
        CircularId::from_vec(self.id.to_u32_digits().clone())
    }
}

impl From<u32> for CircularId{
    fn from(arg: u32) -> CircularId{
        CircularId::from_vec(vec!(arg))
    }
}

impl Serialize for CircularId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let v = self.id.to_u32_digits();
        let mut seq = serializer.serialize_seq(Some(v.len()))?;
        for e in v {
            seq.serialize_element(&e)?;
        }
        seq.end()
    }
}


struct CircularIdVisitor;

impl<'de, > Visitor<'de> for CircularIdVisitor {
    type Value = CircularId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Circular Id from an array of u32")
    }

    fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>
        {
            //let size = seq.size_hint().ok_or(serde::de::Error::custom("Deserialization requires size"))?;
            let mut v = Vec::new();
            loop{
                match seq.next_element(){
                    Ok(result)=>{
                        match result{
                            Some(element) => {
                                v.push(element)
                            },
                            None => {
                                return Ok(CircularId::from_vec(v))
                            },
                        }
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
    }
}

impl<'de> Deserialize<'de> for CircularId {
    fn deserialize<D>(deserializer: D) -> Result<CircularId, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CircularIdVisitor)
    }
}