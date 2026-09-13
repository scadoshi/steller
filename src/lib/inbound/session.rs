//! The per-connection session: read frames, dispatch commands, push replies.
//!
//! A [`Session`] owns both socket halves until [`Session::split`] divides it. The
//! [`ReadHalf`] parses and runs commands, then sends reply bytes down an mpsc. The
//! [`WriteHalf`] owns the socket's write end and drains that mpsc to the wire.
//!
//! That split is what makes pub/sub work. A `PUBLISH` from any session drops bytes into a
//! subscriber's mpsc, and the subscriber's `WriteHalf` delivers them while its `ReadHalf`
//! is still blocked reading from its own client. [`Session::repl`] wires the two together.

use crate::{
    domain::{
        channels::{Channels, ChannelsError, Subscriber},
        command::{
            Command,
            channel::{ChannelCommand, ChannelCommandOutcome, SubUnsubInnerEntry},
        },
        ports::{CacheService, ServiceError},
    },
    resp::{
        frame::{Frame, FrameError},
        reply::{Replies, Reply, SimpleInner},
    },
};
use std::{
    collections::HashSet,
    io::{BufReader, BufWriter, Error as IoError, ErrorKind as IoErrorKind, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvError, SendError, Sender, channel},
    },
    thread::{JoinHandle, spawn},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error(transparent)]
    ReadHalfError(#[from] ReadHalfError),
    #[error(transparent)]
    WriteHalfError(#[from] WriteHalfError),
}

#[derive(Debug, Error)]
pub enum ReadHalfError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Send(#[from] SendError<Vec<u8>>),
}

#[derive(Debug, Error)]
pub enum WriteHalfError {
    #[error(transparent)]
    Recv(#[from] RecvError),
}

#[derive(Debug)]
pub struct SessionReader<R: Read> {
    inner: BufReader<R>,
    buf: Vec<u8>,
}

impl<R: Read> SessionReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            buf: Vec::new(),
        }
    }

    pub fn read(&mut self) -> std::io::Result<usize> {
        let mut new = [0u8; 1024];
        let len = self.inner.read(&mut new)?;
        self.buf.extend_from_slice(&new[..len]);
        Ok(len)
    }

    pub fn parse_frame(&mut self) -> Result<Frame, FrameError> {
        match Frame::parse_one(&self.buf) {
            Ok((frame, bytes)) => {
                let consumed = self.buf.len() - bytes.len();
                self.buf.drain(..consumed);
                Ok(frame)
            }
            Err(e) => {
                if !matches!(e, FrameError::Incomplete) {
                    self.buf.clear();
                }
                Err(e)
            }
        }
    }
}

/// A connected client, owning both socket halves until [`split`](Session::split) runs.
#[derive(Debug)]
pub struct Session<R: Read, W: Write, CS: CacheService> {
    id: u32,
    reader: SessionReader<R>,
    writer: BufWriter<W>,
    cache_service: CS,
    subscriptions: HashSet<Vec<u8>>,
    global_channels: Channels,
}

/// The reading side of a split session. Holds the parser, the cache service, this
/// session's id and subscriptions, a handle to the shared [`Channels`] registry, and the
/// sending end of the reply mpsc. Replies are queued here, never written to the socket.
pub struct ReadHalf<R: Read, CS: CacheService> {
    id: u32,
    reader: SessionReader<R>,
    cache_service: CS,
    sender: Sender<Vec<u8>>,
    subscriptions: HashSet<Vec<u8>>,
    global_channels: Channels,
}

/// Build a registry [`Subscriber`] from a read half. The cloned sender is what lets a
/// `PUBLISH` fan-out land directly in this session's outbound mpsc.
impl<R: Read, CS: CacheService> From<&ReadHalf<R, CS>> for Subscriber {
    fn from(value: &ReadHalf<R, CS>) -> Self {
        Subscriber::new(value.id, value.sender.clone())
    }
}

impl<R: Read, CS: CacheService> ReadHalf<R, CS> {
    /// Queue reply bytes on the outbound mpsc for the [`WriteHalf`] thread to drain.
    /// Errors only once that receiver is gone.
    pub fn send(&self, bytes: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        self.sender.send(bytes)
    }

    /// Read the next complete [`Frame`], pulling more bytes when the buffer holds only
    /// part of one. `Ok(None)` is a clean EOF. A malformed frame gets a `-ERR` reply and
    /// is skipped; the session keeps going.
    pub fn get_frame(&mut self) -> Result<Option<Frame>, ReadHalfError> {
        loop {
            match self.reader.parse_frame() {
                Ok(frame) => return Ok(Some(frame)),
                Err(FrameError::Incomplete) => {
                    if self.reader.read()? == 0 {
                        return Ok(None);
                    }
                    continue;
                }
                Err(e) => {
                    let reply = Reply::SimpleError(SimpleInner::sanitized(format!("ERR {}", e)));
                    self.sender.send(reply.to_bytes())?;
                    continue;
                }
            }
        }
    }

    /// Read the next frame and parse it into a [`Command`]. An unparseable command gets
    /// a `-ERR` reply and is skipped. `Ok(None)` is a clean EOF.
    pub fn get_command(&mut self) -> Result<Option<Command>, ReadHalfError> {
        loop {
            match self.get_frame()?.map(Command::try_from) {
                Some(Ok(cmd)) => return Ok(Some(cmd)),
                Some(Err(e)) => {
                    let reply = Reply::SimpleError(SimpleInner::sanitized(format!("ERR {}", e)));
                    self.sender.send(reply.to_bytes())?;
                    continue;
                }
                None => return Ok(None),
            }
        }
    }

    /// Handle a pub/sub command against the shared registry and this session's
    /// subscriptions, returning the issuer's outcome.
    ///
    /// The registry mutation runs first because it can fail; `subscriptions` is only
    /// updated after it succeeds, and the cumulative count comes off that set. PUBLISH
    /// serializes its `["message", channel, payload]` push here and hands the registry raw
    /// bytes to fan out.
    pub fn execute_channel_command(
        &mut self,
        command: ChannelCommand,
    ) -> Result<ChannelCommandOutcome, ChannelsError> {
        let outcome = match command {
            ChannelCommand::Subscribe { channel_ids } => {
                let mut inner = Vec::new();
                for id in channel_ids {
                    // update global channel handler
                    // do this first since it can fail
                    self.global_channels
                        .subscribe(id.clone(), (&*self).into())?;
                    // update session specific tracker
                    self.subscriptions.insert(id.clone());
                    let subscription_count = self.subscriptions.len();
                    inner.push(SubUnsubInnerEntry::new(id, subscription_count));
                }
                ChannelCommandOutcome::Subscribe { inner }
            }
            ChannelCommand::Unsubscribe { channel_ids } => {
                // No-arg UNSUBSCRIBE means "every channel this session is in". Snapshot the
                // set into an owned list first, since the loop below mutates it.
                let targets: Vec<Vec<u8>> = if channel_ids.is_empty() {
                    self.subscriptions.iter().cloned().collect()
                } else {
                    channel_ids
                };
                let mut inner = Vec::new();
                if targets.is_empty() {
                    // No-arg UNSUBSCRIBE while subscribed to nothing: one null-channel ack.
                    inner.push(SubUnsubInnerEntry::new_null(self.subscriptions.len()));
                } else {
                    for id in targets {
                        // update global channel handler
                        // do this first since it can fail
                        self.global_channels.unsubscribe(id.clone(), self.id)?;
                        // update session specific tracker
                        self.subscriptions.remove(&id);
                        let subscription_count = self.subscriptions.len();
                        inner.push(SubUnsubInnerEntry::new(id, subscription_count));
                    }
                }
                ChannelCommandOutcome::Unsubscribe { inner }
            }
            ChannelCommand::Publish {
                channel_id,
                message,
            } => {
                let message = Reply::Array(vec![
                    Reply::BulkString(b"message".to_vec()),
                    Reply::BulkString(channel_id.clone()),
                    Reply::BulkString(message),
                ])
                .to_bytes();
                ChannelCommandOutcome::Publish {
                    sent_count: self
                        .global_channels
                        .publish(message, channel_id.as_slice())?,
                }
            }
        };
        Ok(outcome)
    }

    /// Remove this session from every channel it joined. Runs once after the repl loop
    /// ends so no dead senders linger in the registry. Results come back per channel so
    /// one poisoned removal doesn't abort the rest.
    pub fn unsubscribe_from_all(&self) -> Vec<Result<(), ChannelsError>> {
        self.subscriptions
            .iter()
            .map(|sub| self.global_channels.unsubscribe(sub, self.id))
            .collect()
    }
}

/// The writing side of a split session, and the only owner of the socket's write end. Its
/// thread blocks on the reply mpsc and writes whatever arrives, whether that's a command
/// reply from its own [`ReadHalf`] or a pub/sub push fanned in from another session.
pub struct WriteHalf<W: Write + Send> {
    writer: BufWriter<W>,
    receiver: Receiver<Vec<u8>>,
}

impl<W: Write + Send + 'static> WriteHalf<W> {
    /// Block until the next buffer of reply bytes arrives. `Err` means every sender is
    /// gone and the session with them, which ends the writer thread.
    pub fn recv(&self) -> Result<Vec<u8>, RecvError> {
        self.receiver.recv()
    }
}

impl<R: Read, W: Write + Send + 'static, CS: CacheService> Session<R, W, CS> {
    /// Build a session over a connected stream's two halves, the shared cache service,
    /// and a handle to the channel registry. Starts with no subscriptions.
    pub fn new(
        id: u32,
        reader: R,
        writer: W,
        cache_service: CS,
        global_channels: Channels,
    ) -> Self {
        let writer = BufWriter::new(writer);
        let reader = SessionReader::new(reader);
        let subscriptions = HashSet::new();
        Self {
            id,
            reader,
            writer,
            cache_service,
            subscriptions,
            global_channels,
        }
    }

    /// Consume the session, create the reply mpsc, and hand the fields to the two halves.
    /// The [`ReadHalf`] keeps the sender, the [`WriteHalf`] the receiver.
    pub fn split(self) -> (ReadHalf<R, CS>, WriteHalf<W>) {
        let Session {
            id,
            reader,
            writer,
            cache_service,
            subscriptions,
            global_channels,
        } = self;
        let (sender, receiver) = channel::<Vec<u8>>();
        let rh = ReadHalf {
            id,
            reader,
            cache_service,
            sender,
            subscriptions,
            global_channels,
        };
        let wh = WriteHalf { writer, receiver };
        (rh, wh)
    }

    /// Run the session to completion. Spawns the writer thread to drain the reply mpsc
    /// and runs the reader loop here.
    ///
    /// Every way out of that loop, whether shutdown, clean EOF, a send failure, or a read
    /// error, funnels through [`unsubscribe_from_all`], so a disconnecting session never
    /// leaves dead senders behind. The writer thread is joined before this returns.
    ///
    /// [`unsubscribe_from_all`]: ReadHalf::unsubscribe_from_all
    pub fn repl(self, shutdown_signal: Arc<AtomicBool>) -> Result<(), SessionError> {
        let id = self.id;
        let (mut rh, mut wh) = self.split();
        let mut handles = Vec::<JoinHandle<()>>::new();

        // writer thread. The shutdown check only runs between messages, since `recv()`
        // parks; the real exit signal is the sender dropping when the reader half goes.
        let write_shutdown = shutdown_signal.clone();
        handles.push(spawn(move || {
            loop {
                if write_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                match wh.recv() {
                    Ok(msg) => match wh.writer.write_all(&msg).and_then(|_| wh.writer.flush()) {
                        Ok(_) => {}
                        Err(e) => eprintln!("failed to write: {}", e),
                    },
                    Err(_) => {
                        println!("client {} has disconnected", id);
                        break;
                    }
                }
            }
        }));

        // reader thread
        let result: Result<(), ReadHalfError> = loop {
            if shutdown_signal.load(Ordering::Relaxed) {
                break Ok(());
            }
            match rh.get_command() {
                Ok(Some(Command::Cache(cc))) => {
                    let reply = Reply::from(
                        match rh
                            .cache_service
                            .execute_logged(cc)
                            .map_err(ReadHalfError::Service)
                        {
                            Ok(outcome) => outcome,
                            Err(e) => break Err(e),
                        },
                    );
                    match rh.send(reply.to_bytes()) {
                        Ok(_) => continue,
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(Some(Command::Channel(command))) => {
                    let outcome = match rh
                        .execute_channel_command(command)
                        .map_err(ServiceError::from)
                        .map_err(ReadHalfError::from)
                    {
                        Ok(outcome) => outcome,
                        Err(e) => break Err(e),
                    };
                    let replies = Replies::from(outcome);
                    match rh.send(replies.to_bytes()) {
                        Ok(_) => continue,
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(Some(Command::Ping { message })) => {
                    let m = message.unwrap_or(Reply::SimpleString(SimpleInner::pong()).to_bytes());
                    match rh.send(m) {
                        Ok(_) => continue,
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(None) => {
                    println!("client {} disconnected", id);
                    break Ok(());
                }
                Err(ReadHalfError::Io(e))
                    if matches!(e.kind(), IoErrorKind::TimedOut | IoErrorKind::WouldBlock) =>
                {
                    continue;
                }
                Err(e) => break Err(e),
            }
        };
        rh.unsubscribe_from_all().iter().for_each(|r| {
            if let Err(e) = r {
                eprintln!("failed to unsubscribe: {}", e)
            }
        });

        // Drop the read half, and with it this session's `Sender`, before joining. The
        // writer thread is parked inside `recv()`, which only returns once every sender is
        // gone; it never loops back to its own shutdown check. Holding `rh` across the join
        // deadlocks shutdown for as long as a client stays connected.
        drop(rh);

        for h in handles {
            let _ = h.join();
        }

        result.map_err(SessionError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{cache::Cache, command::channel::ChannelCommand, service::Service},
        test_support::{RecordingRepo, SharedWriter},
    };
    use std::io::Cursor;

    type TestService = Service<RecordingRepo>;

    /// Build a `(ReadHalf, WriteHalf)` pair sharing `channels`. The reader has an empty
    /// input stream (these tests drive `execute_channel_command` directly, not the repl
    /// loop); the writer is a `SharedWriter` whose `WriteHalf` receiver we keep to inspect
    /// what got pushed to this session.
    fn read_half(
        id: u32,
        channels: Channels,
    ) -> (
        ReadHalf<Cursor<Vec<u8>>, TestService>,
        WriteHalf<SharedWriter>,
    ) {
        let service = Service::new(Cache::default(), RecordingRepo::default());
        let session = Session::new(
            id,
            Cursor::new(Vec::new()),
            SharedWriter::default(),
            service,
            channels,
        );
        session.split()
    }

    fn counts(outcome: &ChannelCommandOutcome) -> Vec<usize> {
        match outcome {
            ChannelCommandOutcome::Subscribe { inner }
            | ChannelCommandOutcome::Unsubscribe { inner } => {
                inner.iter().map(|e| e.subscription_count).collect()
            }
            ChannelCommandOutcome::Publish { .. } => panic!("not a sub/unsub outcome"),
        }
    }

    fn channel_ids(outcome: &ChannelCommandOutcome) -> Vec<Option<Vec<u8>>> {
        match outcome {
            ChannelCommandOutcome::Subscribe { inner }
            | ChannelCommandOutcome::Unsubscribe { inner } => {
                inner.iter().map(|e| e.channel_id.clone()).collect()
            }
            ChannelCommandOutcome::Publish { .. } => panic!("not a sub/unsub outcome"),
        }
    }

    // ---------- subscribe ----------

    #[test]
    fn subscribe_emits_one_entry_per_channel_with_climbing_count() {
        let (mut rh, _wh) = read_half(1, Channels::new());
        let outcome = rh
            .execute_channel_command(ChannelCommand::subscribe(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ]))
            .unwrap();
        assert_eq!(
            channel_ids(&outcome),
            vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec())
            ]
        );
        assert_eq!(counts(&outcome), vec![1, 2, 3]);
    }

    #[test]
    fn subscribe_count_is_session_total_across_calls() {
        let (mut rh, _wh) = read_half(1, Channels::new());
        rh.execute_channel_command(ChannelCommand::subscribe(vec![b"a".to_vec()]))
            .unwrap();
        let outcome = rh
            .execute_channel_command(ChannelCommand::subscribe(vec![b"b".to_vec()]))
            .unwrap();
        // second call starts from the running total of 1, so b is the session's 2nd channel
        assert_eq!(counts(&outcome), vec![2]);
    }

    #[test]
    fn resubscribe_same_channel_does_not_climb() {
        let (mut rh, _wh) = read_half(1, Channels::new());
        rh.execute_channel_command(ChannelCommand::subscribe(vec![b"foo".to_vec()]))
            .unwrap();
        let outcome = rh
            .execute_channel_command(ChannelCommand::subscribe(vec![b"foo".to_vec()]))
            .unwrap();
        // already subscribed, so the session set didn't grow
        assert_eq!(counts(&outcome), vec![1]);
    }

    // ---------- unsubscribe ----------

    #[test]
    fn unsubscribe_descends_count() {
        let (mut rh, _wh) = read_half(1, Channels::new());
        rh.execute_channel_command(ChannelCommand::subscribe(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]))
        .unwrap();
        let outcome = rh
            .execute_channel_command(ChannelCommand::unsubscribe(vec![
                b"a".to_vec(),
                b"c".to_vec(),
            ]))
            .unwrap();
        // remove a -> {b,c}=2 ; remove c -> {b}=1
        assert_eq!(counts(&outcome), vec![2, 1]);
    }

    #[test]
    fn unsubscribe_no_args_leaves_every_channel() {
        let channels = Channels::new();
        let (mut rh, _wh) = read_half(1, channels.clone());
        rh.execute_channel_command(ChannelCommand::subscribe(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]))
        .unwrap();

        let outcome = rh
            .execute_channel_command(ChannelCommand::unsubscribe(Vec::<Vec<u8>>::new()))
            .unwrap();

        // one ack per channel, count draining to zero regardless of removal order
        assert_eq!(counts(&outcome), vec![2, 1, 0]);
        let mut acked: Vec<Vec<u8>> = channel_ids(&outcome).into_iter().flatten().collect();
        acked.sort();
        assert_eq!(acked, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

        // registry is now empty for all three
        assert_eq!(channels.publish(b"x".to_vec(), b"a").unwrap(), 0);
        assert_eq!(channels.publish(b"x".to_vec(), b"b").unwrap(), 0);
        assert_eq!(channels.publish(b"x".to_vec(), b"c").unwrap(), 0);
    }

    #[test]
    fn unsubscribe_no_args_while_subscribed_to_nothing_acks_null_channel() {
        let (mut rh, _wh) = read_half(1, Channels::new());
        let outcome = rh
            .execute_channel_command(ChannelCommand::unsubscribe(Vec::<Vec<u8>>::new()))
            .unwrap();
        match outcome {
            ChannelCommandOutcome::Unsubscribe { inner } => {
                assert_eq!(inner.len(), 1);
                assert_eq!(inner[0].channel_id, None);
                assert_eq!(inner[0].subscription_count, 0);
            }
            _ => panic!("expected Unsubscribe"),
        }
    }

    // ---------- publish (end to end through two sessions) ----------

    #[test]
    fn publish_delivers_message_array_to_subscriber_and_counts() {
        let channels = Channels::new();
        let (mut subscriber, sub_wh) = read_half(1, channels.clone());
        let (mut publisher, _pub_wh) = read_half(2, channels.clone());

        subscriber
            .execute_channel_command(ChannelCommand::subscribe(vec![b"foo".to_vec()]))
            .unwrap();
        let outcome = publisher
            .execute_channel_command(ChannelCommand::publish(b"foo", b"bar"))
            .unwrap();

        assert!(matches!(
            outcome,
            ChannelCommandOutcome::Publish { sent_count: 1 }
        ));

        let expected = Reply::Array(vec![
            Reply::BulkString(b"message".to_vec()),
            Reply::BulkString(b"foo".to_vec()),
            Reply::BulkString(b"bar".to_vec()),
        ])
        .to_bytes();
        assert_eq!(sub_wh.recv().unwrap(), expected);
    }

    #[test]
    fn publish_to_no_subscribers_reaches_zero() {
        let (mut publisher, _wh) = read_half(1, Channels::new());
        let outcome = publisher
            .execute_channel_command(ChannelCommand::publish(b"foo", b"bar"))
            .unwrap();
        assert!(matches!(
            outcome,
            ChannelCommandOutcome::Publish { sent_count: 0 }
        ));
    }

    // ---------- disconnect cleanup ----------

    #[test]
    fn unsubscribe_from_all_removes_session_from_registry() {
        let channels = Channels::new();
        let (mut rh, _wh) = read_half(1, channels.clone());
        rh.execute_channel_command(ChannelCommand::subscribe(vec![
            b"foo".to_vec(),
            b"bar".to_vec(),
        ]))
        .unwrap();

        let results = rh.unsubscribe_from_all();
        assert!(results.iter().all(|r| r.is_ok()));

        // nothing left to reach on either channel
        assert_eq!(channels.publish(b"x".to_vec(), b"foo").unwrap(), 0);
        assert_eq!(channels.publish(b"x".to_vec(), b"bar").unwrap(), 0);
    }
}
