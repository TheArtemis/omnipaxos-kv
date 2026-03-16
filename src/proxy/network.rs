use futures::{SinkExt, StreamExt};
use log::*;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::common::kv::{ClientId, NodeId};
use crate::common::messages::{ClientMessage, ProxyMessage, RegistrationMessage, ServerMessage};
use crate::common::utils::{frame_registration_connection, frame_servers_connection};

type FromProxyServerConnection = Framed<
    FramedRead<tokio::net::tcp::OwnedReadHalf, LengthDelimitedCodec>,
    ServerMessage,
    (),
    Bincode<ServerMessage, ()>,
>;

type ToProxyServerConnection = Framed<
    FramedWrite<tokio::net::tcp::OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ProxyMessage,
    Bincode<(), ProxyMessage>,
>;

fn frame_proxy_server_connection(
    stream: TcpStream,
) -> (FromProxyServerConnection, ToProxyServerConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromProxyServerConnection::new(stream, Bincode::default()),
        ToProxyServerConnection::new(sink, Bincode::default()),
    )
}

pub struct Network {
    server_connections: Vec<Option<ServerConnection>>,
    client_connections: Arc<Mutex<HashMap<ClientId, ClientConnection>>>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    server_message_sender: Sender<ServerMessage>,
    pub client_messages: tokio::sync::mpsc::Receiver<(ClientId, ClientMessage)>,
    pub server_messages: tokio::sync::mpsc::Receiver<ServerMessage>,
}

const RETRY_SERVER_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

impl Network {
    pub async fn new(
        listen_address: SocketAddr,
        servers: Vec<(NodeId, String)>,
        batch_size: usize,
    ) -> Self {
        let mut server_connections = vec![];
        let max_server_id = *servers.iter().map(|(id, _)| id).max().unwrap() as usize;
        server_connections.resize_with(max_server_id + 1, Default::default);
        let (client_message_sender, client_messages) = mpsc::channel(batch_size);
        let (server_message_sender, server_messages) = mpsc::channel(batch_size);
        let mut network = Self {
            server_connections,
            client_connections: Arc::new(Mutex::new(HashMap::new())),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            client_message_sender,
            server_message_sender,
            client_messages,
            server_messages,
        };
        network.spawn_connection_listener(listen_address);
        network.initialize_server_connections(servers).await;
        network
    }

    fn spawn_connection_listener(&self, listen_address: SocketAddr) {
        let client_sender = self.client_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        let client_connections = self.client_connections.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New client connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        let client_connections = client_connections.clone();
                        let client_sender = client_sender.clone();
                        let max_client_id_handle = max_client_id_handle.clone();
                        tokio::spawn(async move {
                            Self::handle_incoming_client_connection(
                                tcp_stream,
                                client_sender,
                                max_client_id_handle,
                                batch_size,
                                client_connections,
                            )
                            .await;
                        });
                    }
                    Err(e) => error!("Error listening for new client connection: {:?}", e),
                }
            }
        });
    }

    async fn handle_incoming_client_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage)>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
        batch_size: usize,
        client_connections: Arc<Mutex<HashMap<ClientId, ClientConnection>>>,
    ) {
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        match registration_message {
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified client {next_client_id} for proxy");
                let underlying_stream = registration_connection.into_inner().into_inner();
                let client_connection = ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    batch_size,
                    client_message_sender,
                );
                client_connections
                    .lock()
                    .unwrap()
                    .insert(next_client_id, client_connection);
            }
            Some(Ok(other)) => {
                warn!("Unexpected registration from client: {other:?}");
            }
            Some(Err(err)) => error!("Error deserializing handshake: {:?}", err),
            None => info!("Connection to unidentified source dropped"),
        }
    }

    async fn initialize_server_connections(&mut self, servers: Vec<(NodeId, String)>) {
        info!("Establishing server connections");
        let mut connection_tasks = Vec::with_capacity(servers.len());
        for (server_id, server_addr_str) in &servers {
            let server_address = server_addr_str
                .to_socket_addrs()
                .expect("Unable to resolve server IP")
                .next()
                .unwrap();
            let server_message_sender = self.server_message_sender.clone();
            let task = tokio::spawn(Self::get_server_connection(
                *server_id,
                server_address,
                self.batch_size,
                server_message_sender,
            ));
            connection_tasks.push(task);
        }
        let finished_tasks = futures::future::join_all(connection_tasks).await;
        for (i, result) in finished_tasks.into_iter().enumerate() {
            match result {
                Ok(to_server_conn) => {
                    let connected_server_id = servers[i].0;
                    info!("Connected to server {connected_server_id}");
                    let server_idx = connected_server_id as usize;
                    self.server_connections[server_idx] = Some(to_server_conn);
                }
                Err(err) => {
                    let failed_server = servers[i].0;
                    panic!("Unable to establish connection to server {failed_server}: {err}")
                }
            }
        }
    }

    async fn get_server_connection(
        server_id: NodeId,
        server_address: SocketAddr,
        batch_size: usize,
        incoming_messages: Sender<ServerMessage>,
    ) -> ServerConnection {
        let mut retry_connection = interval(RETRY_SERVER_CONNECTION_TIMEOUT);
        loop {
            retry_connection.tick().await;
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ProxyRegister)
                        .await
                        .expect("Couldn't send registration to server");
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break ServerConnection::new(
                        server_id,
                        underlying_stream,
                        batch_size,
                        incoming_messages,
                    );
                }
                Err(e) => error!("Unable to connect to server {server_id}: {e}"),
            }
        }
    }

    pub async fn send_to_server(&mut self, to: NodeId, msg: ProxyMessage) {
        match self.server_connections.get_mut(to as usize) {
            Some(connection_slot) => match connection_slot {
                Some(connection) => {
                    if let Err(err) = connection.send(msg).await {
                        warn!("Couldn't send msg to server {to}: {err}");
                        self.server_connections[to as usize] = None;
                    }
                }
                None => error!("Not connected to server {to}"),
            },
            None => error!("Sending to unexpected server {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        let mut connections = self.client_connections.lock().unwrap();
        match connections.get_mut(&to) {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    pub fn send_to_all_clients(&mut self, msg: ServerMessage) {
        let client_ids: Vec<ClientId> = self
            .client_connections
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for client_id in client_ids {
            self.send_to_client(client_id, msg.clone());
        }
    }

    pub fn shutdown(&mut self) {
        for (_, client_connection) in self.client_connections.lock().unwrap().drain() {
            client_connection.close();
        }
        let connection_count = self.server_connections.len();
        for server_connection in self.server_connections.drain(..) {
            if let Some(connection) = server_connection {
                connection.close();
            }
        }
        for _ in 0..connection_count {
            self.server_connections.push(None);
        }
    }
}

struct ServerConnection {
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: Sender<ProxyMessage>,
}

impl ServerConnection {
    pub fn new(
        server_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<ServerMessage>,
    ) -> Self {
        let (reader, mut writer) = frame_proxy_server_connection(connection);
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send(m).await {
                                break;
                            }
                        }
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });

        let (message_tx, mut message_rx) = mpsc::channel(batch_size);
        let writer_task = tokio::spawn(async move {
            while let Some(msg) = message_rx.recv().await {
                if let Err(err) = writer.feed(msg).await {
                    error!("Couldn't send message to server {server_id}: {err}");
                    break;
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to server {server_id}: {err}");
                    break;
                }
            }
        });

        ServerConnection {
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub async fn send(
        &mut self,
        msg: ProxyMessage,
    ) -> Result<(), mpsc::error::SendError<ProxyMessage>> {
        self.outgoing_messages.send(msg).await
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

struct ClientConnection {
    client_id: ClientId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ClientId, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((client_id, m)).await {
                                break;
                            }
                        }
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });

        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            while let Some(msg) = message_rx.recv().await {
                if let Err(err) = writer.feed(msg).await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    break;
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    break;
                }
            }
        });

        ClientConnection {
            client_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}
