//! The pub/sub command family and the outcomes its handlers produce.
//!
//! [`ChannelCommand`] is the parsed form of `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.
//! The per-session router handles it rather than the cache service, because it needs the
//! session's id, its sender, and the shared registry. The handler returns a
//! [`ChannelCommandOutcome`] that the inbound layer turns into wire replies.

/// A parsed pub/sub command.
#[derive(Debug, Clone, PartialEq)]
#[expect(
    missing_docs,
    reason = "each field is the channel list, channel, or message its variant's doc names"
)]
pub enum ChannelCommand {
    /// Join one or more channels. An empty channel list is rejected at parse time.
    Subscribe { channel_ids: Vec<Vec<u8>> },
    /// Leave the listed channels; an empty list means "every channel this session is in".
    Unsubscribe { channel_ids: Vec<Vec<u8>> },
    /// Broadcast `message` to every subscriber of `channel_id`.
    Publish {
        channel_id: Vec<u8>,
        message: Vec<u8>,
    },
}

impl ChannelCommand {
    /// A `SUBSCRIBE` to `channel_ids`. Nothing here rejects an empty list; the parser does.
    pub fn subscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        Self::Subscribe {
            channel_ids: channel_ids.into(),
        }
    }

    /// An `UNSUBSCRIBE` from `channel_ids`, where an empty list means every channel.
    pub fn unsubscribe(channel_ids: impl Into<Vec<Vec<u8>>>) -> Self {
        Self::Unsubscribe {
            channel_ids: channel_ids.into(),
        }
    }

    /// A `PUBLISH` of `message` to `channel_id`.
    pub fn publish(channel_id: impl Into<Vec<u8>>, message: impl Into<Vec<u8>>) -> Self {
        Self::Publish {
            channel_id: channel_id.into(),
            message: message.into(),
        }
    }
}

/// One channel's entry in a subscribe or unsubscribe outcome: the channel acted on, plus
/// the session's running subscription count right after. Redis reports a running total, so
/// the count climbs across a multi-channel `SUBSCRIBE` and descends on `UNSUBSCRIBE`.
///
/// `channel_id` is `None` for exactly one case Redis defines: a no-arg `UNSUBSCRIBE` while
/// subscribed to nothing, which acks with a null channel and a count of 0.
#[derive(Debug, Clone)]
pub struct SubUnsubInnerEntry {
    /// The channel acted on, or `None` for the null-channel ack.
    pub channel_id: Option<Vec<u8>>,
    /// How many channels the session is subscribed to once this entry's change has
    /// landed.
    pub subscription_count: usize,
}

impl SubUnsubInnerEntry {
    /// An entry naming a concrete channel.
    pub fn new(channel_id: impl Into<Vec<u8>>, subscription_count: usize) -> Self {
        Self {
            channel_id: Some(channel_id.into()),
            subscription_count,
        }
    }

    /// The null-channel entry: a no-arg `UNSUBSCRIBE` while subscribed to no channels.
    pub fn new_null(subscription_count: usize) -> Self {
        Self {
            channel_id: None,
            subscription_count,
        }
    }
}

/// The result of handling a [`ChannelCommand`], before it hits the wire. Subscribe and
/// unsubscribe carry one entry per channel. Publish carries how many subscribers it
/// reached.
#[derive(Debug, Clone)]
pub enum ChannelCommandOutcome {
    /// A `SUBSCRIBE` that went through.
    Subscribe {
        /// One entry per channel, in the order the command listed them.
        inner: Vec<SubUnsubInnerEntry>,
    },
    /// An `UNSUBSCRIBE` that went through.
    Unsubscribe {
        /// One entry per channel left, or the single null-channel entry when there was
        /// nothing to leave.
        inner: Vec<SubUnsubInnerEntry>,
    },
    /// A `PUBLISH` that went through.
    Publish {
        /// Subscribers the message reached. Subscribers whose session had already gone
        /// are pruned and not counted.
        sent_count: u32,
    },
}
