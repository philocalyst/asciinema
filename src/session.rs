use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use futures_util::future;
use futures_util::stream::StreamExt;
use nix::sys::wait::{WaitPidFlag, WaitStatus};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use tokio::io;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::error;

use crate::config::Key;
use crate::notifier::Notifier;
use crate::pty::{self, Pty};
use crate::tty::{RawTty, TtySize, TtyTheme};
use crate::util::Utf8Decoder;

const BUF_SIZE: usize = 128 * 1024;

#[derive(Clone)]
pub enum Event {
    Output(Duration, String),
    Input(Duration, String),
    Resize(Duration, TtySize),
    Marker(Duration, String),
    Exit(Duration, i32),
}

#[derive(Clone)]
pub struct Metadata {
    pub time: SystemTime,
    pub term: TermInfo,
    pub idle_time_limit: Option<f64>,
    pub command: Option<String>,
    pub title: Option<String>,
    pub env: HashMap<String, String>,
}

#[derive(Clone)]
pub struct TermInfo {
    pub type_: Option<String>,
    pub version: Option<String>,
    pub size: TtySize,
    pub theme: Option<TtyTheme>,
}

struct Session<N: Notifier> {
    epoch: Instant,
    events_tx: mpsc::Sender<Event>,
    input_decoder: Utf8Decoder,
    keys: KeyBindings,
    notifier: N,
    output_decoder: Utf8Decoder,
    pause_time: Option<Duration>,
    prefix_mode: bool,
    time_offset: Duration,
    capture_input: bool,
    tty_size: TtySize,
}

#[async_trait]
pub trait Output: Send {
    async fn event(&mut self, event: Event) -> io::Result<()>;
    async fn finish(&mut self) -> io::Result<()>;
}

/// An [`Output`], plus how the session treats it.
///
/// Sinks are fed concurrently, in the order the events happen. A sink which
/// fails is dropped from the fan-out.
pub struct Sink {
    output: Box<dyn Output>,
    capture_input: bool,
}

impl Sink {
    pub fn new(output: impl Output + 'static) -> Self {
        Self {
            output: Box::new(output),
            capture_input: false,
        }
    }

    /// Feeds the output keyboard input too. Input is read from the terminal
    /// only when some sink asks for it, and reaches only the sinks which do.
    pub fn capture_input(mut self, capture_input: bool) -> Self {
        self.capture_input = capture_input;

        self
    }

    async fn event(&mut self, event: &Event) -> io::Result<()> {
        if matches!(event, Event::Input(..)) && !self.capture_input {
            return Ok(());
        }

        self.output.event(event.clone()).await
    }
}

pub async fn run<S: AsRef<str>, T: RawTty + ?Sized, N: Notifier>(
    command: &[S],
    extra_env: &HashMap<String, String>,
    tty: &mut T,
    sinks: Vec<Sink>,
    keys: KeyBindings,
    notifier: N,
) -> anyhow::Result<i32> {
    let epoch = Instant::now();
    let (events_tx, events_rx) = mpsc::channel::<Event>(1024);
    let winsize = tty.get_size();
    let capture_input = sinks.iter().any(|sink| sink.capture_input);
    let pty = pty::spawn(command, winsize, extra_env)?;
    let forwarder = tokio::spawn(forward_events(events_rx, sinks));

    let session = Session {
        epoch,
        events_tx,
        input_decoder: Utf8Decoder::new(),
        keys,
        notifier,
        output_decoder: Utf8Decoder::new(),
        pause_time: None,
        prefix_mode: false,
        capture_input,
        time_offset: Duration::from_micros(0),
        tty_size: winsize.into(),
    };

    let result = session.run(pty, tty).await;

    // Wait for the sinks to be finished before reporting the session's exit
    // status, so that the recording is flushed and closed either way.
    let _ = forwarder.await;

    result
}

/// Feeds session events to all sinks until the session ends, then finishes
/// them.
async fn forward_events(mut events_rx: mpsc::Receiver<Event>, mut sinks: Vec<Sink>) {
    while let Some(event) = events_rx.recv().await {
        let results = future::join_all(sinks.iter_mut().map(|sink| sink.event(&event))).await;

        sinks = sinks
            .into_iter()
            .zip(results)
            .filter_map(|(sink, result)| match result {
                Ok(()) => Some(sink),

                Err(e) => {
                    error!("output failed: {e:?}");
                    None
                }
            })
            .collect();
    }

    for mut sink in sinks {
        if let Err(e) = sink.output.finish().await {
            error!("output finish failed: {e:?}");
        }
    }
}

impl<N: Notifier> Session<N> {
    async fn run<T: RawTty + ?Sized>(mut self, pty: Pty, tty: &mut T) -> anyhow::Result<i32> {
        let mut signals =
            Signals::new([SIGWINCH, SIGINT, SIGTERM, SIGQUIT, SIGHUP, SIGALRM, SIGCHLD])?;

        // Kept on the heap so the session future, and the futures holding it,
        // stay small enough for the default thread stack.
        let mut output_buf = vec![0u8; BUF_SIZE];
        let mut input_buf = vec![0u8; BUF_SIZE];
        let mut input = BytesMut::with_capacity(BUF_SIZE);
        let mut output = BytesMut::with_capacity(BUF_SIZE);
        let mut wait_status = None;

        loop {
            tokio::select! {
                result = pty.read(&mut output_buf) => {
                    let n = result?;

                    if n > 0 {
                        self.handle_output(&output_buf[..n]).await;
                        output.extend_from_slice(&output_buf[0..n]);
                    } else {
                        break;
                    }
                }

                result = pty.write(&input), if !input.is_empty() => {
                    let n = result?;
                    input.advance(n);
                }

                result = tty.read(&mut input_buf) => {
                    let n = result?;

                    if n > 0 {
                        if self.handle_input(&input_buf[..n]).await {
                            input.extend_from_slice(&input_buf[..n]);
                        }
                    } else {
                        break;
                    }
                }

                result = tty.write(&output), if !output.is_empty() => {
                    let n = result?;
                    output.advance(n);
                }

                Some(signal) = signals.next() => {
                    match signal {
                        SIGWINCH => {
                            let winsize = tty.get_size();
                            pty.resize(winsize);
                            self.handle_resize(winsize.into()).await;
                        }

                        SIGINT | SIGTERM | SIGQUIT | SIGHUP => {
                            pty.kill();
                        }

                        SIGCHLD => {
                            if let Ok(status) = pty.wait(Some(WaitPidFlag::WNOHANG)).await {
                                if status != WaitStatus::StillAlive {
                                    wait_status = Some(status);
                                    break;
                                }
                            }
                        }

                        _ => {}
                    }
                }
            }
        }

        while let Ok(n) = pty.read(&mut output_buf).await {
            if n > 0 {
                self.handle_output(&output_buf[..n]).await;
                output.extend_from_slice(&output_buf[0..n]);
            } else {
                break;
            }
        }

        if !output.is_empty() {
            let _ = tty.write_all(&output).await;
        }

        let wait_status = match wait_status {
            Some(ws) => ws,
            None => pty.wait(None).await?,
        };

        let status = match wait_status {
            WaitStatus::Exited(_pid, status) => status,
            WaitStatus::Signaled(_pid, signal, ..) => 128 + signal as i32,
            _ => 1,
        };

        self.handle_exit(status).await;

        Ok(status)
    }

    async fn handle_output(&mut self, data: &[u8]) {
        if self.pause_time.is_none() {
            let text = self.output_decoder.feed(data);

            if !text.is_empty() {
                let event = Event::Output(self.elapsed_time(), text);
                self.send_session_event(event).await;
            }
        }
    }

    async fn handle_input(&mut self, data: &[u8]) -> bool {
        let prefix_key = self.keys.prefix.as_ref();
        let pause_key = self.keys.pause.as_ref();
        let add_marker_key = self.keys.add_marker.as_ref();

        if !self.prefix_mode && prefix_key.is_some_and(|key| data == key) {
            self.prefix_mode = true;
            return false;
        }

        if self.prefix_mode || prefix_key.is_none() {
            self.prefix_mode = false;

            if pause_key.is_some_and(|key| data == key) {
                if let Some(pt) = self.pause_time {
                    self.pause_time = None;
                    self.time_offset += self.elapsed_time() - pt;
                    self.notify("Resumed recording").await;
                } else {
                    self.pause_time = Some(self.elapsed_time());
                    self.notify("Paused recording").await;
                }

                return false;
            } else if add_marker_key.is_some_and(|key| data == key) {
                let event = Event::Marker(self.elapsed_time(), "".to_owned());
                self.send_session_event(event).await;
                self.notify("Marker added").await;
                return false;
            }
        }

        if self.capture_input && self.pause_time.is_none() {
            let text = self.input_decoder.feed(data);

            if !text.is_empty() {
                let event = Event::Input(self.elapsed_time(), text);
                self.send_session_event(event).await;
            }
        }

        true
    }

    async fn handle_resize(&mut self, tty_size: TtySize) {
        if tty_size != self.tty_size {
            let event = Event::Resize(self.elapsed_time(), tty_size);
            self.send_session_event(event).await;
            self.tty_size = tty_size;
        }
    }

    async fn handle_exit(&mut self, status: i32) {
        let event = Event::Exit(self.elapsed_time(), status);
        self.send_session_event(event).await;
    }

    fn elapsed_time(&self) -> Duration {
        if let Some(pause_time) = self.pause_time {
            pause_time
        } else {
            self.epoch.elapsed() - self.time_offset
        }
    }

    async fn send_session_event(&mut self, event: Event) {
        self.events_tx
            .send(event)
            .await
            .expect("session event send should succeed");
    }

    async fn notify<S: ToString>(&mut self, text: S) {
        self.notifier
            .notify(text.to_string())
            .await
            .expect("notification should succeed");
    }
}

pub struct KeyBindings {
    pub prefix: Key,
    pub pause: Key,
    pub add_marker: Key,
}

impl Default for KeyBindings {
    fn default() -> Self {
        Self {
            prefix: None,
            pause: Some(vec![0x1c]), // ^\
            add_marker: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use nix::pty::Winsize;

    use super::*;
    use crate::notifier::NullNotifier;

    /// An output which remembers what it was fed.
    #[derive(Default)]
    struct TestOutput {
        events: Arc<Mutex<Vec<Event>>>,
    }

    /// A tty which yields one burst of input, and then nothing ever again.
    struct TestTty(Mutex<Option<Vec<u8>>>);

    #[async_trait]
    impl Output for TestOutput {
        async fn event(&mut self, event: Event) -> io::Result<()> {
            self.events.lock().unwrap().push(event);

            Ok(())
        }

        async fn finish(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl RawTty for TestTty {
        fn get_size(&self) -> Winsize {
            Winsize {
                ws_row: 24,
                ws_col: 80,
                ws_xpixel: 0,
                ws_ypixel: 0,
            }
        }

        async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
            if let Some(input) = self.0.lock().unwrap().take() {
                buf[..input.len()].copy_from_slice(&input);

                return Ok(input.len());
            }

            pending().await
        }

        async fn write(&self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }
    }

    fn test_sink(capture_input: bool) -> (Sink, Arc<Mutex<Vec<Event>>>) {
        let output = TestOutput::default();
        let events = output.events.clone();

        (Sink::new(output).capture_input(capture_input), events)
    }

    fn input_texts(events: &Mutex<Vec<Event>>) -> Vec<String> {
        events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                Event::Input(_, text) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    /// Runs a session which reads a single line from the tty.
    async fn read_line(input: &str, sinks: Vec<Sink>) -> i32 {
        run(
            &["sh", "-c", "read value"],
            &HashMap::new(),
            &mut TestTty(Mutex::new(Some(input.as_bytes().to_vec()))),
            sinks,
            KeyBindings::default(),
            NullNotifier,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn input_reaches_only_the_sinks_asking_for_it() {
        let (listener, listened) = test_sink(true);
        let (bystander, witnessed) = test_sink(false);

        let status = read_line("hi\n", vec![listener, bystander]).await;

        assert_eq!(status, 0);
        assert_eq!(input_texts(&listened), ["hi\n"]);
        assert!(input_texts(&witnessed).is_empty());
        assert!(!witnessed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn input_is_not_captured_when_no_sink_wants_it() {
        let (sink, events) = test_sink(false);

        read_line("secret\n", vec![sink]).await;

        assert!(input_texts(&events).is_empty());
    }
}
