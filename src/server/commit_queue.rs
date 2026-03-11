use omnipaxos_kv::common::kv::{ClientId, Command, CommandId};
use std::collections::VecDeque;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitState {
    Pending,
    Safe,
}

pub struct CommitQueue {
    commands: VecDeque<(Command, CommitState)>,
}

impl CommitQueue {
    pub fn new() -> Self {
        Self {
            commands: VecDeque::new(),
        }
    }

    pub fn push(&mut self, command: Command, state: CommitState) {
        self.commands.push_back((command, state));
    }

    pub fn drain_safe_to_commit_commands(&mut self) -> Vec<Command> {
        let mut drained = Vec::new();
        while let Some((_, state)) = self.commands.front() {
            match state {
                CommitState::Safe => {
                    let (cmd, _) = self.commands.pop_front().expect("front just checked");
                    drained.push(cmd);
                }
                CommitState::Pending => break,
            }
        }
        drained
    }

    fn update_state(&mut self, command: Command, state: CommitState) {
        if let Some((_, s)) = self
            .commands
            .iter_mut()
            .find(|(c, _)| c.client_id == command.client_id && c.id == command.id)
        {
            *s = state;
        }
    }

    pub fn set_safe_to_commit(&mut self, command: Command) {
        self.update_state(command, CommitState::Safe);
    }

    pub fn get_command_by_key(
        &self,
        client_id: ClientId,
        command_id: CommandId,
    ) -> Option<Command> {
        self.commands
            .iter()
            .find(|(c, _)| c.client_id == client_id && c.id == command_id)
            .map(|(c, _)| c.clone())
    }

    pub fn remove_by_key(&mut self, client_id: ClientId, command_id: CommandId) {
        if let Some(pos) = self
            .commands
            .iter()
            .position(|(c, _)| c.client_id == client_id && c.id == command_id)
        {
            self.commands.remove(pos);
        }
    }
}
