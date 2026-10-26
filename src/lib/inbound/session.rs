//! The per-connection session: read frames, dispatch commands, push replies.
//!
//! A [`Session`] owns both socket halves until [`Session::split`] divides it. The
//! [`ReadHalf`] parses and runs commands, then sends reply bytes down an mpsc. The
//! [`WriteHalf`] owns the socket's write end and drains that mpsc to the wire.
//!
//! That split is what makes pub/sub work. A `PUBLISH` from any session drops bytes into a
//! subscriber's mpsc, and the subscriber's `WriteHalf` delivers them while its `ReadHalf`
//! is still blocked reading from its own client. [`Session::repl`] wires the two together.

use std::collections::HashSet;
use std::fmt;
use std::io::{BufReader, BufWriter, Error as IoError, ErrorKind as IoErrorKind, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvError, SendError, Sender};
use std::thread::{self, JoinHandle};

use thiserror::Error;

use crate::domain::channels::{Channels, ChannelsError, Subscriber};
use crate::domain::command::Command;
use crate::domain::command::channel::{ChannelCommand, ChannelCommandOutcome, SubUnsubInnerEntry};
use crate::domain::ports::{Execute, ExecuteError};
use crate::resp::frame::{Frame, FrameError};
use crate::resp::reply::{Replies, Reply, SimpleInner};

/// Why [`Session::repl`] ended with an error rather than a clean disconnect or shutdown.
#[derive(Debug, Error)]
pub enum SessionError {
    /// The reader loop stopped on an error.
    #[error(transparent)]
    ReadHalfError(#[from] ReadHalfError),
    /// The writer side failed. [`Session::repl`] does not produce this; its writer thread
    /// logs its own failures.
    #[error(transparent)]
    WriteHalfError(#[from] WriteHalfError),
}

/// Failure on the reading side of a session, which ends the reader loop.
#[derive(Debug, Error)]
pub enum ReadHalfError {
    /// Reading from the socket failed. [`Session::repl`] skips timeouts and stops on
    /// anything else.
    #[error(transparent)]
    Io(#[from] IoError),
    /// Running a command failed in the cache, the log, or the channel registry.
    #[error(transparent)]
    Executor(#[from] ExecuteError),
    /// The reply mpsc is closed, so the [`WriteHalf`] is gone.
    #[error(transparent)]
    Send(#[from] SendError<Vec<u8>>),
}

/// Failure on the writing side of a session.
#[derive(Debug, Error)]
pub enum WriteHalfError {
    /// Every sender on the reply mpsc is gone.
    #[error(transparent)]
    Recv(#[from] RecvError),
}

/// A buffered socket reader that keeps the bytes of any frame not yet complete.
///
/// Frames can arrive split across reads or several to a read, so bytes accumulate here
/// until [`SessionReader::parse_frame`] can take a whole frame off the front.
#[derive(Debug)]
pub struct SessionReader<R> {
    inner: BufReader<R>,
    buf: Vec<u8>,
}

impl<R: Read> SessionReader<R> {
    /// Wraps `reader` with an empty frame buffer.
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            buf: Vec::new(),
        }
    }

    /// Read up to 1 KiB from the socket onto the end of the buffer, returning how many bytes
    /// arrived. `Ok(0)` means EOF.
    ///
    /// # Errors
    ///
    /// Whatever the underlying [`Read::read`] returns, including a timeout when the socket
    /// has a read timeout set.
    pub fn read(&mut self) -> std::io::Result<usize> {
        let mut new = [0u8; 1024];
        let len = self.inner.read(&mut new)?;
        #[expect(clippy::indexing_slicing, reason = "Read guarantees len <= new.len()")]
        self.buf.extend_from_slice(&new[..len]);
        Ok(len)
    }

    /// Take one complete frame off the front of the buffer.
    ///
    /// # Errors
    ///
    /// Whatever [`Frame::parse_one`] returns. On [`FrameError::Incomplete`] the buffer is
    /// kept so the next read can finish the frame. On any other error the whole buffer is
    /// discarded, including any frames queued behind the bad one.
    pub fn parse_frame(&mut self) -> Result<Frame, FrameError> {
        match Frame::parse_one(&self.buf) {
            Ok((frame, bytes)) => {
                let consumed = self.buf.len().saturating_sub(bytes.len());
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
pub struct Session<R: Read, W: Write, E: Execute> {
    id: u32,
    reader: SessionReader<R>,
    writer: BufWriter<W>,
    executor: E,
    subscriptions: HashSet<Vec<u8>>,
    global_channels: Channels,
}

/// The reading side of a split session. Holds the parser, the cache service, this
/// session's id and subscriptions, a handle to the shared [`Channels`] registry, and the
/// sending end of the reply mpsc. Replies are queued here, never written to the socket.
pub struct ReadHalf<R, E> {
    id: u32,
    reader: SessionReader<R>,
    executor: E,
    sender: Sender<Vec<u8>>,
    subscriptions: HashSet<Vec<u8>>,
    global_channels: Channels,
}

/// Build a registry [`Subscriber`] from a read half. The cloned sender is what lets a
/// `PUBLISH` fan-out land directly in this session's outbound mpsc.
impl<R: Read, E: Execute> From<&ReadHalf<R, E>> for Subscriber {
    fn from(value: &ReadHalf<R, E>) -> Self {
        Subscriber::new(value.id, value.sender.clone())
    }
}

impl<R, E> fmt::Debug for ReadHalf<R, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHalf")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<R: Read, E: Execute> ReadHalf<R, E> {
    /// Queue reply bytes on the outbound mpsc for the [`WriteHalf`] thread to drain.
    /// Errors only once that receiver is gone.
    ///
    /// # Errors
    ///
    /// [`SendError`], holding the unsent bytes, once the [`WriteHalf`] has been dropped.
    pub fn send(&self, bytes: Vec<u8>) -> Result<(), SendError<Vec<u8>>> {
        self.sender.send(bytes)
    }

    /// Read the next complete [`Frame`], pulling more bytes when the buffer holds only
    /// part of one. `Ok(None)` is a clean EOF. A malformed frame gets a `-ERR` reply and
    /// is skipped; the session keeps going.
    ///
    /// # Errors
    ///
    /// [`ReadHalfError::Io`] when a socket read fails, including a read timeout, which the
    /// caller decides whether to retry. [`ReadHalfError::Send`] when the `-ERR` reply for a
    /// malformed frame cannot be queued because the [`WriteHalf`] is gone. A malformed frame
    /// is not itself an error.
    pub fn read_frame(&mut self) -> Result<Option<Frame>, ReadHalfError> {
        loop {
            match self.reader.parse_frame() {
                Ok(frame) => return Ok(Some(frame)),
                Err(FrameError::Incomplete) => {
                    if self.reader.read()? == 0 {
                        return Ok(None);
                    }
                }
                Err(e) => {
                    let reply = Reply::SimpleError(SimpleInner::sanitized(format!("ERR {e}")));
                    self.sender.send(reply.to_bytes())?;
                }
            }
        }
    }

    /// Read the next frame and parse it into a [`Command`]. An unparseable command gets
    /// a `-ERR` reply and is skipped. `Ok(None)` is a clean EOF.
    ///
    /// # Errors
    ///
    /// Whatever [`ReadHalf::read_frame`] returns, plus [`ReadHalfError::Send`] when the
    /// `-ERR` reply for an unparseable command cannot be queued. A bad command is not itself
    /// an error.
    pub fn read_command(&mut self) -> Result<Option<Command>, ReadHalfError> {
        loop {
            match self.read_frame()?.map(Command::try_from) {
                Some(Ok(cmd)) => return Ok(Some(cmd)),
                Some(Err(e)) => {
                    let reply = Reply::SimpleError(SimpleInner::sanitized(format!("ERR {e}")));
                    self.sender.send(reply.to_bytes())?;
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
    ///
    /// # Errors
    ///
    /// [`ChannelsError::MutexPoisoned`] if the registry lock is poisoned. In a
    /// multi-channel `SUBSCRIBE` or `UNSUBSCRIBE`, the channels handled before the failure
    /// stay changed in both the registry and this session's set.
    pub fn execute_channel_command(
        &mut self,
        command: ChannelCommand,
    ) -> Result<ChannelCommandOutcome, ChannelsError> {
        let outcome = match command {
            ChannelCommand::Subscribe { channel_ids } => {
                let mut inner = Vec::new();
                for id in channel_ids {
                    self.global_channels
                        .subscribe(id.clone(), (&*self).into())?;
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
                        self.global_channels.unsubscribe(id.clone(), self.id)?;
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
pub struct WriteHalf<W: Write> {
    writer: BufWriter<W>,
    receiver: Receiver<Vec<u8>>,
}

impl<W: Write> fmt::Debug for WriteHalf<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHalf").finish_non_exhaustive()
    }
}

impl<W: Write + Send + 'static> WriteHalf<W> {
    /// Block until the next buffer of reply bytes arrives. `Err` means every sender is
    /// gone and the session with them, which ends the writer thread.
    ///
    /// # Errors
    ///
    /// [`RecvError`] once the [`ReadHalf`] and every subscriber clone of its sender have
    /// been dropped.
    pub fn recv(&self) -> Result<Vec<u8>, RecvError> {
        self.receiver.recv()
    }
}

impl<R: Read, W: Write + Send + 'static, E: Execute> Session<R, W, E> {
    /// A session over a connected stream's two halves, with no subscriptions yet.
    pub fn new(id: u32, reader: R, writer: W, executor: E, global_channels: Channels) -> Self {
        let writer = BufWriter::new(writer);
        let reader = SessionReader::new(reader);
        let subscriptions = HashSet::new();
        Self {
            id,
            reader,
            writer,
            executor,
            subscriptions,
            global_channels,
        }
    }

    /// Split into halves joined by a reply mpsc: the reader gets the sender, the writer
    /// the receiver.
    pub fn split(self) -> (ReadHalf<R, E>, WriteHalf<W>) {
        let Session {
            id,
            reader,
            writer,
            executor,
            subscriptions,
            global_channels,
        } = self;
        let (sender, receiver) = mpsc::channel::<Vec<u8>>();
        let rh = ReadHalf {
            id,
            reader,
            executor,
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
    /// # Errors
    ///
    /// [`SessionError::ReadHalfError`] when the reader loop stops on an error: a socket
    /// read failing with anything but a timeout, a command failing in the cache, the log,
    /// or the channel registry, or the reply mpsc closing. Shutdown and a clean EOF return
    /// `Ok`. Writer thread failures are logged, not returned.
    ///
    /// [`unsubscribe_from_all`]: ReadHalf::unsubscribe_from_all
    #[expect(
        clippy::print_stdout,
        clippy::print_stderr,
        reason = "the server's own log lines go to its terminal and the service journal"
    )]
    pub fn repl(self, shutdown_signal: &Arc<AtomicBool>) -> Result<(), SessionError> {
        let id = self.id;
        let (mut rh, mut wh) = self.split();
        let mut handles = Vec::<JoinHandle<()>>::new();

        // writer thread. The shutdown check only runs between messages, since `recv()`
        // parks; the real exit signal is the sender dropping when the reader half goes.
        let write_shutdown = Arc::clone(shutdown_signal);
        handles.push(thread::spawn(move || {
            loop {
                if write_shutdown.load(Ordering::Relaxed) {
                    break;
                }
                if let Ok(msg) = wh.recv() {
                    match wh.writer.write_all(&msg).and_then(|()| wh.writer.flush()) {
                        Ok(()) => {}
                        Err(e) => eprintln!("failed to write: {e}"),
                    }
                } else {
                    println!("client {id} has disconnected");
                    break;
                }
            }
        }));

        // reader loop, on this thread
        let result: Result<(), ReadHalfError> = loop {
            if shutdown_signal.load(Ordering::Relaxed) {
                break Ok(());
            }
            match rh.read_command() {
                Ok(Some(Command::Cache(cc))) => {
                    let reply = Reply::from(
                        match rh
                            .executor
                            .execute_logged(cc)
                            .map_err(ReadHalfError::Executor)
                        {
                            Ok(outcome) => outcome,
                            Err(e) => break Err(e),
                        },
                    );
                    match rh.send(reply.to_bytes()) {
                        Ok(()) => {}
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(Some(Command::Channel(command))) => {
                    let outcome = match rh
                        .execute_channel_command(command)
                        .map_err(ExecuteError::from)
                        .map_err(ReadHalfError::from)
                    {
                        Ok(outcome) => outcome,
                        Err(e) => break Err(e),
                    };
                    let replies = Replies::from(outcome);
                    match rh.send(replies.to_bytes()) {
                        Ok(()) => {}
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(Some(Command::Ping { message })) => {
                    let m = message
                        .unwrap_or_else(|| Reply::SimpleString(SimpleInner::pong()).to_bytes());
                    match rh.send(m) {
                        Ok(()) => {}
                        Err(e) => break Err(ReadHalfError::Send(e)),
                    }
                }
                Ok(None) => {
                    println!("client {id} disconnected");
                    break Ok(());
                }
                Err(ReadHalfError::Io(e))
                    if matches!(e.kind(), IoErrorKind::TimedOut | IoErrorKind::WouldBlock) => {}
                Err(e) => break Err(e),
            }
        };
        for r in &rh.unsubscribe_from_all() {
            if let Err(e) = r {
                eprintln!("failed to unsubscribe: {e}");
            }
        }

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
    use std::io::Cursor;

    use super::*;
    use crate::domain::cache::Cache;
    use crate::domain::command::channel::ChannelCommand;
    use crate::domain::executor::Executor;
    use crate::test_support::{RecordingRepo, SharedWriter};

    type TestService = Executor<RecordingRepo>;

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
        let service = Executor::new(Cache::default(), RecordingRepo::default());
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
        let (mut publisher, _pub_wh) = read_half(2, channels);

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
        assert!(results.iter().all(std::result::Result::is_ok));

        // nothing left to reach on either channel
        assert_eq!(channels.publish(b"x".to_vec(), b"foo").unwrap(), 0);
        assert_eq!(channels.publish(b"x".to_vec(), b"bar").unwrap(), 0);
    }
}
