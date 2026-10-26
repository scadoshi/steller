use crate::domain::time::Seconds;

/// The three things `TTL` can say about a key, before the wire turns them into
/// `:-2`, `:-1`, or `:n`.
#[derive(Debug)]
pub enum TtlOutcome {
    /// The key is missing, or had expired and was dropped by this read. Sent as `:-2`.
    KeyNotFound,
    /// The key exists with no deadline. Sent as `:-1`.
    TtlNotFound,
    /// Time left, rounded up to whole seconds. Sent as `:n`.
    Some(Seconds),
}

/// What a cache command produced, in domain terms. The inbound layer maps each variant to
/// one RESP [`Reply`](crate::resp::reply::Reply).
#[derive(Debug)]
pub enum CommandOutcome {
    /// A `GET` result: the value as a bulk string, or `None` for a null bulk.
    Value(Option<Vec<u8>>),
    /// A plain `+OK`, as `SET` answers.
    Ok,
    /// A yes or no that goes out as `:1` or `:0`, as `DEL`, the expire commands, and
    /// `PERSIST` answer.
    Bool(bool),
    /// A `TTL` result.
    Ttl(TtlOutcome),
    /// A `PING` reply: `+PONG`, or the message echoed back as a bulk string, byte for byte.
    Pong(Option<Vec<u8>>),
    /// A count sent as `:n`. `EXISTS` uses it.
    Integer(i64),
}
