use env_logger;
use omnipaxos_kv::proxy::proxy::Proxy;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let mut proxy =
        Proxy::from_proxy_config().await.unwrap_or_else(|_| panic!("Failed to create proxy"));
    proxy.run().await;
}
