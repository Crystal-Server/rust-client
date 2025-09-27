use crate::netdata::server_update_callback::ServerUpdateCallback;

#[derive(Debug)]
pub(crate) struct CallbackServerUpdate {
    pub name: String,
    pub callback: ServerUpdateCallback,
}
