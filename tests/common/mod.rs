

use dht_chord::{TCPChord, chord::ChordHandle};
use tokio::time::{sleep, Duration};


pub async fn make_nodes(qty: u32, delay: u32) ->Vec<ChordHandle<String, u32>>{
	let mut v = Vec::new();
	for i in 0..qty{
		let addr = format!("192.168.0.10:{}", 2000 + i);
		let chord = TCPChord::new(addr, i * 2);
		let handle = if i != 0 {
			chord.start(Some(String::from("192.168.0.10:2000"))).await
		}else{
			chord.start(None).await
		};
		v.push(handle.expect("Chords should be able to start"));
		println!("Waiting {delay}ms to spawn next node...");
		sleep(Duration::from_millis(delay.into())).await;
		println!("waiting over...");
	}

	v
}
