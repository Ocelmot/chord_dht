use crate::{ChordProcessor, chord_id::ChordId};


impl<I: ChordId> ChordProcessor<I>{

    /// Stabilize routine has 3 operations, and 2 messages:
    /// 1. stabilize - ask successor for its predecessor. Initiated by timer.
    ///     Message: GetPredecessor - request a node's predecessor
    /// 2. get_predecesssor - reply with this node's predecessor
    ///     Message: Predecessor - a node's predecessor
    /// 3. update_predecessor - if the successor's predecessor is between
    ///    this node and its successor, update.


    pub(crate) async fn stabilize(&mut self){
        // ask successor for its pred. if that node is closer than current successor, update successor
        if let Some(successor_id) = self.finger_table.get(0){
            if let Some(successor) = self.members.get_mut(successor_id){
               //successor.send(ChordMessage::GetPredecessor{from: self.self_id.clone()}).await;
            }
        }
    }
    pub(crate) async fn get_predecessor(&mut self, from: &I){
        if let Some(node) = self.members.get_mut(from){
            //node.send(ChordMessage::Predecessor{of: self.self_id.clone(), is: self.predecessor.clone()}).await;
        }
        
    }
    pub(crate) async fn update_successor(&mut self, node: I, pred: I){
        // get successor, check if update is from successor, if pred is not this node, set successor to that node
        // Since the successor is just the next connected node from us, issue a connectback message to the new successor
        
        // match self.get_successor_node(self.self_id.calculate_finger(0)){
        //     Some(successor) => {
        //         if *successor.get_id() == node {
        //             if pred != self.self_id{
        //                 // TODO: issue dialback message to pred (the new successor)
        //             }
        //         }
        //     },
        //     None => todo!(),
        // }
    }

    pub(crate) async fn notify(&mut self){
        // notify successor about us!
        if let Some(successor_id) = self.finger_table.get(0){
            if let Some(node) = self.members.get_mut(successor_id){
                //node.send(ChordMessage::Notify{from: self.self_id.clone()}).await;
            }
        }
    }

    pub(crate) async fn notified(&mut self, notified_id: I){
        // if we are notified of a new predecessor, check against our curent predecessor id
        // and update if needed
        match &self.predecessor{
            Some(predecessor) => {
                if notified_id.is_between(&predecessor, &self.self_id){
                    self.predecessor = Some(notified_id);
                }
                // otherwise ignore
            },
            None => {
                self.predecessor = Some(notified_id);
            },
        }
    }

    pub(crate) async fn fix_fingers(&mut self){
        // for each finger, try to find the correct node to point to, then replace finger with that node.
        todo!()
    }

    /// test to see if connection for pred id exists.
    /// If not, connection has died, remove predecessor.
    /// predecessor connection will be pinged by that node.
    pub(crate) async fn check_predecessor(&mut self){
        if let Some(predecessor) = &self.predecessor { // if we have a predecessor
            if self.members.get_mut(predecessor).is_none() { // if node is not connected
                self.predecessor = None; // Clear predecessor
            }
        }   
    }
}


