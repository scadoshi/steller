//! The pub/sub subscription registry and message fan-out.
//!
//! [`Channels`] maps each channel name to its [`Subscribers`], behind an `Arc<Mutex>` that
//! every session shares. Cloning shares the inner `Arc`, so all sessions hold a handle to
//! the same registry.
//!
//! The registry only ever moves raw `Vec<u8>`. RESP framing happens in the inbound layer
//! before anything gets here, so a published message is just bytes pushed into each
//! subscriber's channel.

use thiserror::Error;

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex,
        mpsc::{SendError, Sender},
    },
};

/// One session's delivery endpoint: its id, which is the registry key, plus the sending
/// half of the mpsc its `WriteHalf` drains to the socket.
#[derive(Debug)]
pub struct Subscriber {
    id: u32,
    sender: Sender<Vec<u8>>,
}

impl Subscriber {
    pub fn new(id: u32, sender: Sender<Vec<u8>>) -> Self {
        Self { id, sender }
    }

    /// Push raw bytes toward this subscriber's socket. Errors once the receiving
    /// `WriteHalf` is gone, which means the session is too.
    pub fn send(&self, message: impl Into<Vec<u8>>) -> Result<(), SendError<Vec<u8>>> {
        self.sender.send(message.into())
    }
}

/// The subscribers of one channel, keyed by session id. That keying makes unsubscribe and
/// disconnect cleanup O(1), and stops a session registering twice to the same channel.
#[derive(Debug, Default)]
pub struct Subscribers {
    inner: HashMap<u32, Sender<Vec<u8>>>,
}

impl Deref for Subscribers {
    type Target = HashMap<u32, Sender<Vec<u8>>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Subscribers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug, Error)]
pub enum ChannelsError {
    /// A thread panicked while holding the registry lock.
    #[error("mutex was poisoned")]
    MutexPoisoned,
}

/// The shared subscription registry, channel name to [`Subscribers`]. Every clone points
/// at the same inner map.
#[derive(Debug, Default, Clone)]
pub struct Channels {
    channels: Arc<Mutex<HashMap<Vec<u8>, Subscribers>>>,
}

impl Channels {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `subscriber` under `channel_id`, creating the channel on first subscribe.
    /// Idempotent per session id: re-subscribing replaces that session's sender. Counting
    /// a session's subscriptions is the caller's bookkeeping, not the registry's.
    pub fn subscribe(
        &self,
        channel_id: impl Into<Vec<u8>>,
        subscriber: Subscriber,
    ) -> Result<(), ChannelsError> {
        let mut guard = self
            .channels
            .lock()
            .map_err(|_| ChannelsError::MutexPoisoned)?;
        guard
            .entry(channel_id.into())
            .or_default()
            .insert(subscriber.id, subscriber.sender);
        Ok(())
    }

    /// Remove this subscriber from `channel_id`. The channel itself is dropped once its
    /// last subscriber leaves, so the map only holds live channels. A no-op if either the
    /// channel or the subscriber is already gone.
    pub fn unsubscribe(
        &self,
        channel_id: impl AsRef<[u8]>,
        subscriber_id: u32,
    ) -> Result<(), ChannelsError> {
        let mut guard = self
            .channels
            .lock()
            .map_err(|_| ChannelsError::MutexPoisoned)?;
        if let Some(subs) = guard.get_mut(channel_id.as_ref()) {
            subs.remove(&subscriber_id);
            if subs.is_empty() {
                guard.remove(channel_id.as_ref());
            }
        }
        Ok(())
    }

    /// Fan `message` out to every subscriber of `channel_id`, returning how many got it.
    /// Subscribers whose receiver has been dropped are pruned along the way. The message
    /// is delivered verbatim, since the caller already serialized the RESP push.
    pub fn publish(
        &self,
        message: impl AsRef<Vec<u8>>,
        channel_id: &[u8],
    ) -> Result<u32, ChannelsError> {
        let mut sent_count = 0u32;
        let mut guard = self
            .channels
            .lock()
            .map_err(|_| ChannelsError::MutexPoisoned)?;
        if let Some(subs) = guard.get_mut(channel_id) {
            subs.retain(
                |_id, sender| match sender.send(message.as_ref().to_owned()) {
                    Ok(()) => {
                        sent_count = sent_count.saturating_add(1);
                        true
                    }
                    Err(_) => false,
                },
            );
        }
        Ok(sent_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, channel};

    /// Register a fresh subscriber on `channel_id` and hand back the receiver end so the
    /// test can assert what got delivered.
    fn subscribe(channels: &Channels, id: u32, channel_id: &[u8]) -> Receiver<Vec<u8>> {
        let (tx, rx) = channel::<Vec<u8>>();
        channels
            .subscribe(channel_id.to_vec(), Subscriber::new(id, tx))
            .unwrap();
        rx
    }

    #[test]
    fn subscribe_then_publish_delivers_payload() {
        let channels = Channels::new();
        let rx = subscribe(&channels, 1, b"foo");
        let reached = channels.publish(b"hello".to_vec(), b"foo").unwrap();
        assert_eq!(reached, 1);
        assert_eq!(rx.recv().unwrap(), b"hello".to_vec());
    }

    #[test]
    fn publish_to_unknown_channel_reaches_zero() {
        let channels = Channels::new();
        assert_eq!(channels.publish(b"hello".to_vec(), b"nope").unwrap(), 0);
    }

    #[test]
    fn publish_only_reaches_subscribers_of_that_channel() {
        let channels = Channels::new();
        let foo_rx = subscribe(&channels, 1, b"foo");
        let _bar_rx = subscribe(&channels, 2, b"bar");
        assert_eq!(channels.publish(b"hi".to_vec(), b"foo").unwrap(), 1);
        assert_eq!(foo_rx.recv().unwrap(), b"hi".to_vec());
    }

    #[test]
    fn multiple_subscribers_all_receive_and_count() {
        let channels = Channels::new();
        let rx1 = subscribe(&channels, 1, b"foo");
        let rx2 = subscribe(&channels, 2, b"foo");
        assert_eq!(channels.publish(b"yo".to_vec(), b"foo").unwrap(), 2);
        assert_eq!(rx1.recv().unwrap(), b"yo".to_vec());
        assert_eq!(rx2.recv().unwrap(), b"yo".to_vec());
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let channels = Channels::new();
        let rx = subscribe(&channels, 1, b"foo");
        channels.unsubscribe(b"foo", 1).unwrap();
        assert_eq!(channels.publish(b"hi".to_vec(), b"foo").unwrap(), 0);
        // The sender end is gone from the registry, so nothing was delivered.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn resubscribe_same_id_is_idempotent() {
        // Re-subscribing the same session id to the same channel must not double-count.
        let channels = Channels::new();
        let (tx, rx) = channel::<Vec<u8>>();
        channels
            .subscribe(b"foo".to_vec(), Subscriber::new(1, tx.clone()))
            .unwrap();
        channels
            .subscribe(b"foo".to_vec(), Subscriber::new(1, tx))
            .unwrap();
        assert_eq!(channels.publish(b"hi".to_vec(), b"foo").unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), b"hi".to_vec());
    }

    #[test]
    fn publish_prunes_dead_subscribers() {
        let channels = Channels::new();
        let live_rx = subscribe(&channels, 1, b"foo");
        let dead_rx = subscribe(&channels, 2, b"foo");
        drop(dead_rx); // session 2's receiver is gone

        // First publish sees the dead sender, delivers to the live one, prunes the dead.
        assert_eq!(channels.publish(b"one".to_vec(), b"foo").unwrap(), 1);
        assert_eq!(live_rx.recv().unwrap(), b"one".to_vec());

        // Second publish: dead sender already pruned, count stays at 1.
        assert_eq!(channels.publish(b"two".to_vec(), b"foo").unwrap(), 1);
        assert_eq!(live_rx.recv().unwrap(), b"two".to_vec());
    }

    #[test]
    fn unsubscribe_unknown_channel_is_noop() {
        let channels = Channels::new();
        assert!(channels.unsubscribe(b"ghost", 1).is_ok());
    }
}
