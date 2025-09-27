#[derive(Debug, Clone, PartialEq)]
pub enum AdminAction {
    Unban,
    /// Reason, Unix unban time
    Ban(String, i64),
    /// Reason
    Kick(String),
}
