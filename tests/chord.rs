mod common;

use core::panic;
use std::collections::HashSet;
use tokio::time::{sleep, Duration};

use dht_chord::associate::{AssociateRequest, AssociateResponse};





#[tokio::test]
async fn ten_nodes_consistency() {
	let v = common::make_nodes(10, 0).await;

	// wait some amount of time for chord nodes to stabilize
	let wait = 300;
	println!("Waiting {} seconds for chord nodes to stabilize...", wait);
	sleep(Duration::from_secs(wait)).await;
	println!("waiting over...");
	
	let mut successors = HashSet::new();
	let mut predecessors = HashSet::new();
	
	for handle in v{
		let mut assoc = handle.get_associate().await;
		assoc.send_op(AssociateRequest::GetSuccessor).await;
		let succ = if let Some(AssociateResponse::Successor{id: Some((id, addr))}) = assoc.recv_op().await {
			id
		}else {
			panic!("Test failed since node has no successor!");
		};

		assoc.send_op(AssociateRequest::GetPredecessor).await;
		let pred = if let Some(AssociateResponse::Predecessor{id: Some((id, addr))}) = assoc.recv_op().await {
			id
		}else {
			panic!("Test failed since node has no predecessor!");
		};

		if successors.contains(&succ) {
			panic!("Test failed since node's successor has already been seen")
		}else{
			successors.insert(succ);
		}

		if predecessors.contains(&pred) {
			panic!("Test failed since node's predecessor has already been seen")
		}else{
			predecessors.insert(pred);
		}

		let op = assoc.recv_op().await;
		println!("=================");
		if let Some(AssociateResponse::Debug { msg }) = op {
			println!("{}", msg);
		}else{
			panic!("Invalid response: {:?}", op);
		}


	}
}

#[tokio::test]
async fn finger_table_calculation() {
	let v = common::make_nodes(100, 0).await;

	// wait some amount of time for chord nodes to stabilize
	// let wait = 300;
	// println!("Waiting {} seconds for chord nodes to stabilize...", wait);
	// sleep(Duration::from_secs(wait)).await;
	// println!("waiting over..."); 

	let mut origin = v[1].get_associate().await;
	for i in 0..100 {
		origin.send_op(AssociateRequest::Debug).await;
		let op = origin.recv_op().await;
		println!("=================");
		if let Some(AssociateResponse::Debug { msg }) = op {
			println!("{}", msg);
		}else{
			panic!("Invalid response: {:?}", op);
		}
		sleep(Duration::from_secs(5)).await;
	}
	

	

}




#[tokio::test]
async fn test_state_after_stabilization() {
	let v = common::make_nodes(10, 16000).await;

	// wait some amount of time for chord nodes to stabilize
	let wait = 400;
	println!("Waiting {wait} seconds for chord nodes to stabilize...");
	sleep(Duration::from_secs(wait)).await;
	println!("waiting over...");
	
	
	let mut i = 0;
	for handle in v{
		let mut assoc = handle.get_associate().await;
		assoc.send_op(AssociateRequest::Debug).await;
		if let Some(AssociateResponse::Debug { msg }) = assoc.recv_op().await {
			println!("{msg}");
		}else {
			println!("Node {i} failed to respond!");
		};

		i += 1;
	}
}
