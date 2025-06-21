use anyhow::Result;
use protocol::{application::_impl_sync::ServiceApplication};

const CURRENT_SERVICE_ID: u16 = 0x01;
fn main() -> Result<()> {
    env_logger::init();

    let mut sd = ServiceApplication::new(CURRENT_SERVICE_ID);
    sd.init()?;
    sd.start(true)?;
    
    Ok(())
}
