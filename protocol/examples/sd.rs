use std::{env};

use anyhow::Result;
use protocol::sd::{ServiceDiscovery};
use tokio::runtime::Runtime;

async fn _main() -> Result<()>{
    let args: Vec<String> = env::args().collect();
    let service_id = args.get(1)
        .expect("Please provide a service ID")
        .parse::<u16>()
        .expect("Invalid service ID");

    let timeout = args.get(2)
        .expect("Please provide a timeout")
        .parse::<u64>()
        .expect("Invalid timeout");

    let mut sd = ServiceDiscovery::new(service_id);
    sd.init().await?;
    sd.start().await?;
    println!("Service Discovery started with ID: {}, Timeout: {}", service_id, timeout);

    std::thread::sleep(std::time::Duration::from_secs(timeout));
    sd.stop().await;


    println!("Service Discovery finished after {} seconds", timeout);
    Ok(())
}

fn main() -> Result<()> {
    let rt  = Runtime::new()?;

    rt.block_on(async {
        _main().await
    })
}