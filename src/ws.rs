use crate::web::State;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};
use warp::ws::{Message, WebSocket};

pub const MAX_WS_CLIENTS: usize = 512;
const WS_OUTBOUND_BUFFER_CAPACITY: usize = 32;
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct WsClientGate(Arc<Semaphore>);

impl Default for WsClientGate {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(MAX_WS_CLIENTS)))
    }
}

impl WsClientGate {
    pub fn with_capacity(limit: usize) -> Self {
        Self(Arc::new(Semaphore::new(limit)))
    }

    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.0).try_acquire_owned().ok()
    }
}

pub struct WsApiClient {
    state: Arc<State>,
    outbound: Sender<OutboundApiMessage>,
    shutdown: Arc<Notify>,
    listing_changes: Option<LiveHandle>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InboundApiMessage {
    Subscribe { channel: MessageChannel },
    Unsubscribe { channel: MessageChannel },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutboundApiMessage {
    Subscribed {
        channel: MessageChannel,
    },
    Unsubscribed {
        channel: MessageChannel,
    },
    ListingsRevisionChanged {
        revision: u64,
        revision_epoch: String,
    },
    Err {
        message: String,
    },
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
#[serde(rename_all = "snake_case")]
enum MessageChannel {
    ListingChanges,
}

impl WsApiClient {
    fn try_send_outbound(
        sender: &Sender<OutboundApiMessage>,
        shutdown: &Notify,
        msg: OutboundApiMessage,
    ) -> bool {
        match sender.try_send(msg) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                tracing::warn!("closing slow WebSocket consumer: outbound queue full");
                shutdown.notify_waiters();
                false
            }
            Err(TrySendError::Closed(_)) => {
                shutdown.notify_waiters();
                false
            }
        }
    }

    async fn handle(&mut self, msg: InboundApiMessage) {
        match msg {
            InboundApiMessage::Subscribe { channel } => {
                match channel {
                    MessageChannel::ListingChanges => {
                        if self.listing_changes.is_none() {
                            self.listing_changes = Some(
                                tokio::spawn(Self::listing_changes_task(
                                    self.state.clone(),
                                    self.outbound.clone(),
                                    Arc::clone(&self.shutdown),
                                ))
                                .into(),
                            );
                        }
                    }
                };

                // send a message letting the client know they've been subscribed
                let _ = Self::try_send_outbound(
                    &self.outbound,
                    &self.shutdown,
                    OutboundApiMessage::Subscribed { channel },
                );
            }
            InboundApiMessage::Unsubscribe { channel } => {
                match channel {
                    MessageChannel::ListingChanges => {
                        self.listing_changes = None; // drops the task.
                    }
                }

                // send a message letting the client know they've been unsubscribed
                let _ = Self::try_send_outbound(
                    &self.outbound,
                    &self.shutdown,
                    OutboundApiMessage::Unsubscribed { channel },
                );
            }
        }
    }

    pub async fn run(state: Arc<State>, web_socket: WebSocket) {
        let (outbound_sender, mut outbound_receiver) =
            tokio::sync::mpsc::channel(WS_OUTBOUND_BUFFER_CAPACITY);
        let (mut ws_sender, mut ws_receiver) = web_socket.split();
        let shutdown = Arc::new(Notify::new());

        let mut client = Self {
            state,
            outbound: outbound_sender,
            shutdown: Arc::clone(&shutdown),
            listing_changes: None,
        };

        let send_task = Self::send_task(&mut outbound_receiver, &mut ws_sender);
        let recv_task = Self::recv_task(&mut ws_receiver, &mut client);

        // run either send, recv, or slow-consumer shutdown to completion;
        // any exiting is fatal to the ws client.
        tokio::select! {
            _ = send_task => (),
            _ = recv_task => (),
            _ = shutdown.notified() => (),
        }
    }

    async fn send_task(
        outbound_receiver: &mut Receiver<OutboundApiMessage>,
        ws_sender: &mut SplitSink<WebSocket, Message>,
    ) {
        let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;

        loop {
            tokio::select! {
                maybe_msg = outbound_receiver.recv() => {
                    let Some(msg) = maybe_msg else {
                        break;
                    };
                    let Ok(json) = serde_json::to_string(&msg) else {
                        tracing::debug!("failed to serialize outbound message: {:#?}", msg);
                        continue;
                    };

                    if ws_sender.send(Message::text(json)).await.is_err() {
                        break; // can't send. fatal. die
                    }
                }
                _ = heartbeat.tick() => {
                    if ws_sender.send(Message::ping(Vec::new())).await.is_err() {
                        break; // can't ping. fatal. die
                    }
                }
            }
        }
    }

    async fn recv_task(ws_receiver: &mut SplitStream<WebSocket>, client: &mut WsApiClient) {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            if msg.is_close() {
                break;
            }
            // give up if there's an error (as far as I can tell they're fatal anyway)
            if let Ok(msg) = msg.to_str() {
                // only a close message has no to_str
                match serde_json::from_str::<InboundApiMessage>(msg) {
                    Ok(msg) => {
                        client.handle(msg).await;
                    }
                    Err(e) => {
                        if !Self::try_send_outbound(
                            &client.outbound,
                            &client.shutdown,
                            OutboundApiMessage::Err {
                                message: e.to_string(),
                            },
                        ) {
                            break;
                        }
                    }
                };
            }
        }
    }

    async fn listing_changes_task(
        state: Arc<State>,
        sender: Sender<OutboundApiMessage>,
        shutdown: Arc<Notify>,
    ) {
        let mut receiver = state.listings_change_channel.subscribe();
        let revision_epoch = state.revision_epoch().to_string();
        if !Self::try_send_outbound(
            &sender,
            &shutdown,
            OutboundApiMessage::ListingsRevisionChanged {
                revision: state.current_listings_revision(),
                revision_epoch: revision_epoch.clone(),
            },
        ) {
            return;
        }

        loop {
            match receiver.recv().await {
                Ok(revision) => {
                    if !Self::try_send_outbound(
                        &sender,
                        &shutdown,
                        OutboundApiMessage::ListingsRevisionChanged {
                            revision,
                            revision_epoch: revision_epoch.clone(),
                        },
                    ) {
                        break;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    if !Self::try_send_outbound(
                        &sender,
                        &shutdown,
                        OutboundApiMessage::ListingsRevisionChanged {
                            revision: state.current_listings_revision(),
                            revision_epoch: revision_epoch.clone(),
                        },
                    ) {
                        break;
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    }
}

/// A handle to a tokio task that aborts the task when dropped.
struct LiveHandle(AbortHandle);

impl Drop for LiveHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> From<JoinHandle<T>> for LiveHandle {
    fn from(value: JoinHandle<T>) -> Self {
        Self(value.abort_handle())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_consumer_fills_bounded_channel_and_triggers_shutdown_without_panic() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(WS_OUTBOUND_BUFFER_CAPACITY);
        let shutdown = Arc::new(Notify::new());

        for revision in 0..WS_OUTBOUND_BUFFER_CAPACITY {
            assert!(WsApiClient::try_send_outbound(
                &tx,
                &shutdown,
                OutboundApiMessage::ListingsRevisionChanged {
                    revision: revision as u64,
                    revision_epoch: "epoch-test".to_string(),
                },
            ));
        }

        let overflow = WsApiClient::try_send_outbound(
            &tx,
            &shutdown,
            OutboundApiMessage::ListingsRevisionChanged {
                revision: 999,
                revision_epoch: "epoch-test".to_string(),
            },
        );
        assert!(!overflow);
        drop(rx.recv().await);
    }

    #[test]
    fn ws_client_gate_enforces_max_concurrent_cap_and_releases_on_drop() {
        assert_eq!(MAX_WS_CLIENTS, 512);
        let gate = WsClientGate::with_capacity(2);
        let p1 = gate.try_acquire().expect("first permit should succeed");
        let p2 = gate.try_acquire().expect("second permit should succeed");
        assert!(gate.try_acquire().is_none(), "capacity cap should reject 3rd client");
        drop(p1);
        assert!(
            gate.try_acquire().is_some(),
            "dropping permit should free slot"
        );
        drop(p2);
    }

    #[tokio::test]
    async fn duplicate_subscribe_keeps_existing_listing_changes_task() {
        let state = crate::web::test_support::state_with_signature_required().await;
        let (tx, mut rx) = tokio::sync::mpsc::channel(WS_OUTBOUND_BUFFER_CAPACITY);
        let mut client = WsApiClient {
            state,
            outbound: tx,
            shutdown: Arc::new(Notify::new()),
            listing_changes: None,
        };

        client
            .handle(InboundApiMessage::Subscribe {
                channel: MessageChannel::ListingChanges,
            })
            .await;
        tokio::task::yield_now().await;
        assert!(!client
            .listing_changes
            .as_ref()
            .expect("subscribe should spawn listing changes task")
            .0
            .is_finished());

        client
            .handle(InboundApiMessage::Subscribe {
                channel: MessageChannel::ListingChanges,
            })
            .await;
        tokio::task::yield_now().await;

        let mut drained = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            drained.push(msg);
        }
        let expected_epoch = client.state.revision_epoch().to_string();
        let revision_frames: Vec<_> = drained
            .iter()
            .filter(|msg| matches!(msg, OutboundApiMessage::ListingsRevisionChanged { .. }))
            .collect();
        assert_eq!(
            revision_frames.len(),
            1,
            "idempotent Subscribe must not restart snapshot task"
        );
        let serialized = serde_json::to_value(revision_frames[0]).unwrap();
        assert_eq!(serialized["revision_epoch"], expected_epoch);
        drop(client);
    }
}
