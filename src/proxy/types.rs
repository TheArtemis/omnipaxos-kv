use crate::common::kv::{ClientId, CommandId, NodeId};
use crate::common::messages::{FastReply, ServerResult};
use omnipaxos::ballot_leader_election::Ballot;

/// Reply set for one (client_id, request_id): current ballot and replies from replicas.
pub(crate) struct ReplySetState {
    pub(crate) current_ballot: Ballot,
    pub(crate) replies: Vec<FastReply>,
}

pub(crate) struct SlowReplySetState {
    pub(crate) replies: Vec<NodeId>,
    pub(crate) result: Option<ServerResult>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ClientRequestKey {
    pub(crate) client_id: ClientId,
    pub(crate) command_id: CommandId,
}

impl ClientRequestKey {
    pub(crate) fn new(client_id: ClientId, command_id: CommandId) -> Self {
        Self {
            client_id,
            command_id,
        }
    }
}
