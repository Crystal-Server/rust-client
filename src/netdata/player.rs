use std::collections::HashMap;

use crate::netdata::{client_sync::ClientSync, variable::Variable};

/// Contains the data of a connected Player
#[derive(Debug, Clone)]
pub struct Player {
    pub name: String,
    pub room: String,
    pub syncs: Vec<Option<ClientSync>>,
    pub variables: HashMap<String, Variable>,
}
