use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq)]
pub enum DisconnectionType {
    /// Triggers whenever the client disconnect for whatever reason
    Disconnected,
    /// This actually doesn't disconnect the game, but does logout the player.<br>
    /// This will only happen when an admin kicks the player.<br>
    /// The String is the Reason of the kick.
    Kicked(String),
    /// This actually doesn't disconnect the game, but does logout the player.<br>
    /// This will only happen when an admin bans the player.
    /// The String is the Reason of the kick, the DateTime<Utc> is when the player will be unbanned.
    Banned(String, DateTime<Utc>),
}
