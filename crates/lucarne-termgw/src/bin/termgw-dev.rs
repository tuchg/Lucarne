//! termgw-dev — standalone runner for the terminal gateway (manual/e2e test).
//!
//! Connects to the SYSTEM rmux daemon, adopts its sessions, and serves the ws +
//! HTTP gateway. NOT shipped: `lucarned` builds the production router directly
//! into the default product binary; this runner exists only for local
//! end-to-end testing against a real daemon.
//!
//! ```text
//! TERMGW_ADDR=127.0.0.1:7800 TERMGW_WEB=web cargo run -p lucarne-termgw --bin termgw-dev
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use lucarne_rmux::RmuxMonitor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let monitor = Arc::new(RmuxMonitor::connect().await?);
    let adopted = monitor.adopt_all().await?;
    eprintln!("termgw-dev: adopted {} system session(s)", adopted.len());

    let addr: SocketAddr = std::env::var("TERMGW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7800".to_string())
        .parse()?;
    let web_dir = PathBuf::from(std::env::var("TERMGW_WEB").unwrap_or_else(|_| "web".to_string()));

    eprintln!(
        "termgw-dev: serving http://{addr} (web dir: {})",
        web_dir.display()
    );
    lucarne_termgw::serve(monitor, addr, web_dir).await?;
    Ok(())
}
