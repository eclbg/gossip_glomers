use async_trait::async_trait;
use log::{debug, info};
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

const NUM_POLL_RESULTS: usize = 5;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone, Default)]
struct Handler {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    logs: HashMap<String, Vec<(usize, usize)>>,
    committed_offsets: HashMap<String, usize>,
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::default());
    runtime.with_handler(handler).run().await
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: RequestBody = req.body.as_obj().expect("Error deserializing message body");
        match body {
            RequestBody::Send { key, msg } => {
                let mut s = self.state.lock().await;
                // create entry if not exists
                let msgs = s.logs.entry(key.clone()).or_insert_with(|| Vec::new());
                // len + 1 as we still haven't pushed the new message
                let offset = msgs.len() + 1;
                debug!("Will send back offset: {}", offset);
                msgs.push((offset, msg));
                debug!("Currently in logs: {:?}", s.logs);
                let resp = ResponseBody::SendOk { offset };
                debug!("Response: {}", serde_json::to_string(&resp).unwrap());
                return runtime.reply(req, resp).await;
            }
            RequestBody::Poll { offsets } => {
                let mut resp_msgs: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
                let s = self.state.lock().await;
                debug!("Currently in logs: {:?}", s.logs);
                for (key, offset) in offsets.iter() {
                    if let Some(msgs) = s.logs.get(key) {
                        if let Some((first_to_return_idx, _)) = msgs
                            .iter()
                            .enumerate()
                            .filter(|(_, (o, _))| o >= offset)
                            .next()
                        {
                            resp_msgs.insert(
                                key.clone(),
                                msgs.iter().skip(first_to_return_idx).copied().collect(),
                            );
                        } else {
                            debug!("No messages on or after offset")
                        }
                    } else {
                        debug!("Polled for non-existing key {}", key)
                    }
                }
                let resp = ResponseBody::PollOk { msgs: resp_msgs };
                return runtime.reply(req, resp).await;
            }
            RequestBody::CommitOffsets { offsets } => {
                let mut s = self.state.lock().await;
                s.committed_offsets.extend(offsets);
                return runtime.reply_ok(req).await;
            }
            RequestBody::ListCommittedOffsets { keys } => {
                let s = self.state.lock().await;
                let offsets = s
                    .committed_offsets
                    .iter()
                    .filter(|(k, _)| keys.contains(k))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                let resp = ResponseBody::ListCommittedOffsetsOk { offsets };
                return runtime.reply(req, resp).await;
            }
            RequestBody::Init { .. } => Ok(()),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Send {
        key: String,
        msg: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    // CommitOffsetsOk
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}
