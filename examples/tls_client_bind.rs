extern crate fstack_mio as mio;
use std::env;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use fastwebsockets::FragmentCollector;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use http_body_util::Empty;
use hyper::header::CONNECTION;
use hyper::header::UPGRADE;
use hyper::upgrade::Upgraded;
use hyper::Request;
use hyper_util::rt::TokioIo;
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn_local(fut);
    }
}

fn tls_connector() -> Result<TlsConnector> {
    let mut root_cert_store = rustls::RootCertStore::empty();

    root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

async fn connect(domain: &str, remote_ip: String, local_ip: String) -> Result<FragmentCollector<TokioIo<Upgraded>>> {
    let mut addr = String::from(domain);
    addr.push_str(":9443"); // Port number for binance stream
    let remote_addr: SocketAddr = remote_ip.parse().expect("Invalid remote ip");
    let local_addr: SocketAddr = local_ip.parse().expect("Invalid local ip");

    let tcp_stream = mio::sys::unix::TcpStream::connect_from_bind(remote_addr, local_addr).expect("Failed to bind to local ip");

    let tcp_stream = TcpStream::from_std(tcp_stream).expect("Failed to convert to tokio TcpStream");
    let tls_connector = tls_connector().unwrap();

    let domain = rustls::pki_types::ServerName::try_from(domain)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    let tls_stream = tls_connector.connect(domain, tcp_stream).await?;

    let req = Request::builder()
        .method("GET")
        .uri(format!("wss://{}/ws/btcusdt@bookTicker", &addr)) //stream we want to subscribe to
        .header("Host", &addr)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(Empty::<Bytes>::new())?;

    let (ws, _) =
        fastwebsockets::handshake::client(&SpawnExecutor, req, tls_stream).await?;
    Ok(FragmentCollector::new(ws))
}

// RUST_LOG=debug ./target/debug/examples/tls_client_bind 67.228.102.32:443 192.168.8.107:0
fn main() {
    let mut args: Vec<String> = env::args().collect();
    let remote_ip = args[1].clone();
    let local_ip = args[2].clone();
    args.remove(1);
    args.remove(1);

    tokio::fstack_init(args.len(), args);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let local = tokio::task::LocalSet::new();
        local.run_until(async move {
            let domain = "data-stream.binance.com";
            let mut ws = connect(domain, remote_ip, local_ip).await.expect("Failed to connect to binance stream");
            loop {
                let msg = match ws.read_frame().await {
                    Ok(msg) => msg,
                    Err(e) => {
                        println!("Error: {}", e);
                        ws.write_frame(Frame::close_raw(vec![].into())).await.expect("Failed to close");
                        break;
                    }
                };

                match msg.opcode {
                    OpCode::Text => {
                        let payload =
                            String::from_utf8(msg.payload.to_vec()).expect("Invalid UTF-8 data");
                        // Normally deserialise from json here, print just to show it works
                        println!("{:?}", payload);
                    }
                    OpCode::Close => {
                        break;
                    }
                    _ => {}
                }
            }
        }).await;
    })
}
