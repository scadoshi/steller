use crate::domain::time::Seconds;

/// The three things `TTL` can say about a key, before the wire turns them into
/// `:-2`, `:-1`, or `:n`.
#[derive(Debug)]
pub enum TtlOutcome {
    KeyNotFound,
    TtlNotFound,
    Some(Seconds),
}

#[derive(Debug)]
pub enum CommandOutcome {
    Value(Option<Vec<u8>>),
    Ok,
    Bool(bool),
    Ttl(TtlOutcome),
    Pong(Option<Vec<u8>>),
    Integer(i64),
}
