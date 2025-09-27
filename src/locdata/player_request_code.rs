/// The target it should request something from/to
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlayerRequestCode {
    AllGame,
    PlayerId(u64),
}
