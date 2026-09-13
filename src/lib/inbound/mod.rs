//! Turns incoming bytes into a [`crate::domain::command::Command`].
//!
//! [`server`] is the accept loop and the background threads. [`session`] is the
//! per-connection REPL that reads bytes, parses frames, and dispatches.
//!
//! The `From` impls below are the other direction: outcome to wire reply.

use crate::{
    domain::command::{
        channel::{ChannelCommandOutcome, SubUnsubInnerEntry},
        outcome::{CommandOutcome, TtlOutcome},
    },
    resp::reply::{Replies, Reply, SimpleInner},
};

pub mod server;
pub mod session;

type Co = CommandOutcome;
impl From<CommandOutcome> for Reply {
    fn from(value: CommandOutcome) -> Self {
        match value {
            Co::Value(Some(value)) => Self::BulkString(value),
            Co::Value(None) => Self::NullBulk,
            Co::Ok => Self::SimpleString(SimpleInner::ok()),
            Co::Pong(Some(message)) => Self::SimpleString(SimpleInner::sanitized(message)),
            Co::Pong(None) => Self::SimpleString(SimpleInner::pong()),
            Co::Bool(bool) => Self::Integer(bool as i64),
            Co::Ttl(TtlOutcome::KeyNotFound) => Self::Integer(-2),
            Co::Ttl(TtlOutcome::TtlNotFound) => Self::Integer(-1),
            Co::Ttl(TtlOutcome::Some(ttl)) => {
                Self::Integer(i64::try_from(ttl.get()).unwrap_or(i64::MAX))
            }
            Co::Integer(int) => Self::Integer(int),
        }
    }
}

/// A named channel renders as a bulk string. The null sentinel, which only happens on a
/// no-arg `UNSUBSCRIBE` while subscribed to nothing, renders as `$-1`.
fn channel_reply(channel_id: Option<Vec<u8>>) -> Reply {
    match channel_id {
        Some(c) => Reply::BulkString(c),
        None => Reply::NullBulk,
    }
}

type Cco = ChannelCommandOutcome;
/// Map a channel command's outcome to the frames sent back to the issuing session.
///
/// Subscribe and unsubscribe emit one `["(un)subscribe", channel, count]` array per
/// channel, which is why this returns [`Replies`] rather than a single [`Reply`]. Publish
/// emits one `:N`.
///
/// This is only the issuer's acknowledgement. The push to subscribers is built on the
/// publish path and never comes through here.
impl From<ChannelCommandOutcome> for Replies {
    fn from(value: ChannelCommandOutcome) -> Self {
        match value {
            Cco::Subscribe { inner } => {
                let mut replies = Vec::new();
                for SubUnsubInnerEntry {
                    channel_id,
                    subscription_count,
                } in inner
                {
                    replies.push(Reply::Array(vec![
                        Reply::BulkString(b"subscribe".to_vec()),
                        channel_reply(channel_id),
                        Reply::Integer(subscription_count as i64),
                    ]));
                }
                Self { inner: replies }
            }
            Cco::Unsubscribe { inner } => {
                let mut replies = Vec::new();
                for SubUnsubInnerEntry {
                    channel_id,
                    subscription_count,
                } in inner
                {
                    replies.push(Reply::Array(vec![
                        Reply::BulkString(b"unsubscribe".to_vec()),
                        channel_reply(channel_id),
                        Reply::Integer(subscription_count as i64),
                    ]));
                }
                Self { inner: replies }
            }
            Cco::Publish { sent_count } => Self {
                inner: vec![Reply::Integer(i64::from(sent_count))],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::time::Seconds;

    #[test]
    fn value_some_maps_to_bulk_string() {
        assert_eq!(
            Reply::from(Co::Value(Some(b"foo".to_vec()))),
            Reply::BulkString(b"foo".to_vec())
        );
    }

    #[test]
    fn value_none_maps_to_null_bulk() {
        assert_eq!(Reply::from(Co::Value(None)), Reply::NullBulk);
    }

    #[test]
    fn ok_maps_to_simple_string_ok() {
        assert_eq!(Reply::from(Co::Ok), Reply::SimpleString(SimpleInner::ok()));
    }

    #[test]
    fn pong_none_maps_to_simple_string_pong() {
        assert_eq!(
            Reply::from(Co::Pong(None)),
            Reply::SimpleString(SimpleInner::pong())
        );
    }

    #[test]
    fn pong_some_maps_to_simple_string_sanitized() {
        assert_eq!(
            Reply::from(Co::Pong(Some(b"hi".to_vec()))),
            Reply::SimpleString(SimpleInner::sanitized(b"hi".to_vec()))
        );
    }

    #[test]
    fn pong_some_with_crlf_is_sanitized() {
        assert_eq!(
            Reply::from(Co::Pong(Some(b"hi\r\nthere".to_vec()))),
            Reply::SimpleString(SimpleInner::sanitized(b"hi\r\nthere".to_vec()))
        );
    }

    #[test]
    fn bool_true_maps_to_integer_one() {
        assert_eq!(Reply::from(Co::Bool(true)), Reply::Integer(1));
    }

    #[test]
    fn bool_false_maps_to_integer_zero() {
        assert_eq!(Reply::from(Co::Bool(false)), Reply::Integer(0));
    }

    #[test]
    fn ttl_key_not_found_maps_to_integer_negative_two() {
        assert_eq!(
            Reply::from(Co::Ttl(TtlOutcome::KeyNotFound)),
            Reply::Integer(-2)
        );
    }

    #[test]
    fn ttl_ttl_not_found_maps_to_integer_negative_one() {
        assert_eq!(
            Reply::from(Co::Ttl(TtlOutcome::TtlNotFound)),
            Reply::Integer(-1)
        );
    }

    #[test]
    fn ttl_some_maps_to_integer() {
        assert_eq!(
            Reply::from(Co::Ttl(TtlOutcome::Some(Seconds::new(123)))),
            Reply::Integer(123)
        );
    }

    #[test]
    fn ttl_some_overflow_saturates_to_i64_max() {
        assert_eq!(
            Reply::from(Co::Ttl(TtlOutcome::Some(Seconds::new(u64::MAX)))),
            Reply::Integer(i64::MAX)
        );
    }

    #[test]
    fn integer_passes_through() {
        assert_eq!(Reply::from(Co::Integer(42)), Reply::Integer(42));
        assert_eq!(Reply::from(Co::Integer(-7)), Reply::Integer(-7));
    }

    // ---------- ChannelCommandOutcome -> Replies ----------

    #[test]
    fn subscribe_outcome_maps_to_per_channel_arrays() {
        let replies = Replies::from(Cco::Subscribe {
            inner: vec![
                SubUnsubInnerEntry::new(b"foo".to_vec(), 1),
                SubUnsubInnerEntry::new(b"bar".to_vec(), 2),
            ],
        });
        assert_eq!(
            replies.inner,
            vec![
                Reply::Array(vec![
                    Reply::BulkString(b"subscribe".to_vec()),
                    Reply::BulkString(b"foo".to_vec()),
                    Reply::Integer(1),
                ]),
                Reply::Array(vec![
                    Reply::BulkString(b"subscribe".to_vec()),
                    Reply::BulkString(b"bar".to_vec()),
                    Reply::Integer(2),
                ]),
            ]
        );
    }

    #[test]
    fn subscribe_outcome_wire_bytes() {
        let replies = Replies::from(Cco::Subscribe {
            inner: vec![SubUnsubInnerEntry::new(b"foo".to_vec(), 1)],
        });
        assert_eq!(
            replies.to_bytes(),
            b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n"
        );
    }

    #[test]
    fn unsubscribe_outcome_maps_to_per_channel_arrays() {
        let replies = Replies::from(Cco::Unsubscribe {
            inner: vec![SubUnsubInnerEntry::new(b"foo".to_vec(), 0)],
        });
        assert_eq!(
            replies.inner,
            vec![Reply::Array(vec![
                Reply::BulkString(b"unsubscribe".to_vec()),
                Reply::BulkString(b"foo".to_vec()),
                Reply::Integer(0),
            ])]
        );
    }

    #[test]
    fn unsubscribe_all_when_empty_maps_to_null_channel() {
        let replies = Replies::from(Cco::Unsubscribe {
            inner: vec![SubUnsubInnerEntry::new_null(0)],
        });
        assert_eq!(
            replies.inner,
            vec![Reply::Array(vec![
                Reply::BulkString(b"unsubscribe".to_vec()),
                Reply::NullBulk,
                Reply::Integer(0),
            ])]
        );
        assert_eq!(
            replies.to_bytes(),
            b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
        );
    }

    #[test]
    fn publish_outcome_maps_to_single_integer() {
        let replies = Replies::from(Cco::Publish { sent_count: 3 });
        assert_eq!(replies.inner, vec![Reply::Integer(3)]);
        assert_eq!(replies.to_bytes(), b":3\r\n");
    }

    #[test]
    fn empty_subscribe_outcome_maps_to_no_replies() {
        let replies = Replies::from(Cco::Subscribe { inner: vec![] });
        assert!(replies.inner.is_empty());
        assert_eq!(replies.to_bytes(), b"");
    }
}
