use crate::{node::Node, ProcessorData};






pub async fn incoming_connection(pd: &mut ProcessorData, node: Node){
    match node{
        Node::Local { .. } => {},
        Node::Other { ref id, ..} => {
            pd.connections.insert(id.clone(), node);
        },
    }

}