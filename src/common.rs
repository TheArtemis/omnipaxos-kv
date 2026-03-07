pub mod messages {
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};

    use crate::dom::request::DomMessage;

    use super::{
        kv::{ClientId, Command, CommandId, KVCommand},
        utils::Timestamp,
    };

    /// Re-exported for proxy reply-set state and ballot comparison.
    pub use omnipaxos::ballot_leader_election::Ballot;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
        // Used by the proxy to distinguish its connection type on the server.
        ProxyRegister,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(OmniPaxosMessage<Command>),
        LeaderStartSignal(Timestamp),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Append(CommandId, KVCommand),
    }

    impl ClientMessage {
        pub fn command_id(&self) -> CommandId {
            match self {
                ClientMessage::Append(id, _) => *id,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Write(CommandId),
        Read(CommandId, Option<String>),
        StartSignal(Timestamp),
        FastReply(FastReply),
        SlowPathReply(SlowPathReply),
        DeadlineLengthRequestReply(NodeId, u64),
    }

    impl ServerMessage {
        pub fn command_id(&self) -> CommandId {
            match self {
                ServerMessage::Write(id) => *id,
                ServerMessage::Read(id, _) => *id,
                ServerMessage::StartSignal(_) => unimplemented!(),
                ServerMessage::FastReply(fr) => fr.request_id,
                ServerMessage::SlowPathReply(sr) => sr.request_id,
                ServerMessage::DeadlineLengthRequestReply(_, _) => unimplemented!(),
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct FastReply {
        pub ballot: Ballot,
        pub replica_id: NodeId,
        pub client_id: ClientId,
        pub request_id: CommandId,
        pub result: Option<ServerResult>, // None for the followers
        pub hash: super::log_hash::LogHash,
    }

    impl FastReply {
        pub fn is_replica_reply(&self) -> bool {
            self.result.is_none()
        }

        pub fn is_leader_reply(&self) -> bool {
            self.result.is_some()
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerResult {
       Write(CommandId),
       Read(CommandId, Option<String>),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct SlowPathReply {
        pub replica_id: NodeId,
        pub client_id: ClientId,
        pub request_id: CommandId,
        pub result: Option<ServerResult>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct CommitMessage {
        pub client_id: ClientId,
        pub command_id: CommandId,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ProxyMessage {
        Commit(CommitMessage),
        Append(DomMessage),
        DeadlineLengthRequest(u64, String),
    }
}

/// Running set-hash over log entries: SHA-1 per entry, XOR'd into a single value.
/// Equality of set hashes across replicas implies identical log contents (order fixed by deadlines).
pub mod log_hash {
    use serde::{Deserialize, Serialize};

    use super::kv::Command;

    const SHA1_LEN: usize = 20;

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LogHash(pub [u8; SHA1_LEN]);

    impl LogHash {
        /// Empty log hash
        pub fn new() -> Self {
            Self::default()
        }

        /// Hash a single log entry with SHA-1 (deterministic bincode serialization).
        fn hash_entry(command: &Command) -> [u8; SHA1_LEN] {
            use sha1::{Digest, Sha1};
            let bytes = bincode::serialize(command).expect("Command serialization is infallible");
            let digest = Sha1::new().chain_update(bytes).finalize();
            digest.as_slice().try_into().expect("SHA-1 output is 20 bytes")
        }

        #[inline]
        fn xor_into(running: &mut [u8; SHA1_LEN], entry_hash: &[u8; SHA1_LEN]) {
            for i in 0..SHA1_LEN {
                running[i] ^= entry_hash[i];
            }
        }

        pub fn add_entry(&mut self, command: &Command) {
            let h = Self::hash_entry(command);
            Self::xor_into(&mut self.0, &h);
        }

        pub fn remove_entry(&mut self, command: &Command) {
            let h = Self::hash_entry(command);
            Self::xor_into(&mut self.0, &h);
        }

        pub fn replace_entry(&mut self, old_command: &Command, new_command: &Command) {
            self.remove_entry(old_command);
            self.add_entry(new_command);
        }
    }

    impl AsRef<[u8; SHA1_LEN]> for LogHash {
        fn as_ref(&self) -> &[u8; SHA1_LEN] {
            &self.0
        }
    }
}

pub mod kv {
    use omnipaxos::{macros::Entry, storage::Snapshot};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    // Re-export telemetry types so existing `common::kv::*` imports keep working.
    pub use crate::telemetry::{MAX_LATENCY_SAMPLES, NodeMetrics, SystemMetrics};
    /// Backward-compatible alias for [`NodeMetrics`].
    pub type Metrics = NodeMetrics;

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;
    pub type InstanceId = NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub kv_cmd: KVCommand,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum KVCommand {
        Put(String, String),
        Delete(String),
        Get(String),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<String, String>,
        deleted_keys: Vec<String>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys: Vec<String> = Vec::new();
            for e in entries {
                match &e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key.clone(), value.clone());
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key.clone());
                        }
                    }
                    KVCommand::Get(_) => (),
                }
            }
            // remove keys that were put back
            deleted_keys.retain(|k| !snapshotted.contains_key(k));
            Self {
                snapshotted,
                deleted_keys,
            }
        }

        fn merge(&mut self, delta: Self) {
            for (k, v) in delta.snapshotted {
                self.snapshotted.insert(k, v);
            }
            for k in delta.deleted_keys {
                self.snapshotted.remove(&k);
            }
            self.deleted_keys.clear();
        }

        fn use_snapshots() -> bool {
            true
        }
    }
}

pub mod utils {
    use super::messages::*;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

    pub type Timestamp = i64;

    pub type RegistrationConnection = Framed<
        CodecFramed<TcpStream, LengthDelimitedCodec>,
        RegistrationMessage,
        RegistrationMessage,
        Bincode<RegistrationMessage, RegistrationMessage>,
    >;

    pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
        let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
        Framed::new(length_delimited, Bincode::default())
    }

    pub type FromNodeConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClusterMessage,
        (),
        Bincode<ClusterMessage, ()>,
    >;
    pub type ToNodeConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClusterMessage,
        Bincode<(), ClusterMessage>,
    >;

    pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromNodeConnection::new(stream, Bincode::default()),
            ToNodeConnection::new(sink, Bincode::default()),
        )
    }

    // pub type ServerConnection = Framed<
    //     CodecFramed<TcpStream, LengthDelimitedCodec>,
    //     ServerMessage,
    //     ClientMessage,
    //     Bincode<ServerMessage, ClientMessage>,
    // >;

    pub type FromServerConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ServerMessage,
        (),
        Bincode<ServerMessage, ()>,
    >;

    pub type ToServerConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClientMessage,
        Bincode<(), ClientMessage>,
    >;

    pub type FromClientConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClientMessage,
        (),
        Bincode<ClientMessage, ()>,
    >;

    pub type ToClientConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ServerMessage,
        Bincode<(), ServerMessage>,
    >;

    pub fn frame_clients_connection(
        stream: TcpStream,
    ) -> (FromServerConnection, ToServerConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromServerConnection::new(stream, Bincode::default()),
            ToServerConnection::new(sink, Bincode::default()),
        )
    }

    // pub fn frame_clients_connection(stream: TcpStream) -> ServerConnection {
    //     let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    //     Framed::new(length_delimited, Bincode::default())
    // }

    pub fn frame_servers_connection(
        stream: TcpStream,
    ) -> (FromClientConnection, ToClientConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromClientConnection::new(stream, Bincode::default()),
            ToClientConnection::new(sink, Bincode::default()),
        )
    }
}
