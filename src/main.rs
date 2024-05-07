mod core;

use crate::core::client::CrystalServer;

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    for _ in 0..200 {
        tokio::spawn(async {
            let mut cs = CrystalServer::init();
            cs.connect().await;
            loop {
                
            }
        });
    }
}
