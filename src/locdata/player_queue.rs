use std::collections::HashMap;

use integer_hasher::IntMap;

use crate::{locdata::new_sync::NewSync, netdata::optional_variable::OptionalVariable};

#[derive(Default, Debug, Clone)]
pub(crate) struct PlayerQueue {
    pub variables: HashMap<String, OptionalVariable>,
    pub syncs: IntMap<usize, HashMap<String, OptionalVariable>>,
    pub remove_syncs: Vec<usize>,
    pub new_syncs: Vec<NewSync>,
}
