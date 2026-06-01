use serde_json::Value;
use std::collections::HashMap;

pub type AwarenessState = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub struct ClientAwarenessState {
    pub client_id: u64,
    pub state: AwarenessState,
}

pub fn awareness_states_to_array(
    states: &HashMap<u64, AwarenessState>,
) -> Vec<ClientAwarenessState> {
    states
        .iter()
        .map(|(client_id, state)| ClientAwarenessState {
            client_id: *client_id,
            state: state.clone(),
        })
        .collect()
}
