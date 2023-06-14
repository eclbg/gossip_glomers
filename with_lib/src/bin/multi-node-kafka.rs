use async_trait::async_trait;
use log::debug;
use maelstrom::kv::{lin_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_context::context::Context;

type Pair = (usize, usize);
type CommittedOffsets = HashMap<String, usize>;

static COMMITTED_OFFSETS_KEY: &str = "committed_offsets";

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone)]
struct Handler {
    s: Storage,
}

async fn try_main() -> Result<()> {
    // The challenge assignment suggests using the lin-kv service provided by maelstrom. We do need
    // linearizability but only between messages in the same partition (key). It's important to
    // understand the scope of the linearizability guarantee of the lin-kv service. That's not
    // specified in the maelstrom docs so I guess we can assume it's total linearizability? Could we
    // use lin-kv just for the offsets and store the messages locally? And maybe gossip them between
    // nodes? I don't think that would be correct as a client might observe older messages for the
    // first time in more recent polls. Could we do Read + CaS operations on whole lists? This would
    // be correct but feels very brute force. Let's start by trying this just to get the ball
    // rolling.
    let runtime = Runtime::new();
    let handler = Arc::new(Handler {
        s: lin_kv(runtime.clone()),
    });
    runtime.with_handler(handler).run().await
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: RequestBody = req.body.as_obj().expect("Error deserializing message body");
        match body {
            RequestBody::Send { key, msg } => {
                // Read from lin-kv and, if the key is empty,
                // try to write with a CaS operation until it succeeds
                //
                // This could be Err if the key doesn't exist
                let read_res = self.s.get::<Vec<Pair>>(Context::new().0, key.clone()).await;
                let mut msgs = if read_res.is_err() {
                    // If CaS succeeds the next get will surely succeed. If CaS fails it's because
                    // someone else already put something in key, therefore the next read will
                    // succeed. This means we only need to attempt the CaS once and we can ignore
                    // the result.
                    let _ = self
                        .s
                        .cas(
                            Context::new().0,
                            key.clone(),
                            Vec::<Pair>::new(),
                            Vec::<Pair>::new(),
                            true,
                        )
                        .await;
                    let msgs = self
                        .s
                        .get::<Vec<Pair>>(Context::new().0, key.clone())
                        .await
                        .unwrap();
                    msgs
                } else {
                    read_res.unwrap()
                };
                // At this point msgs could be an empty vec
                debug!("{:?}", msgs); // To check that the pattern matching is correct
                let offset = if let Some((last_offset, _)) = msgs.last() {
                    last_offset + 1
                } else {
                    // If msgs is an empty vec, we need to insert offset 1
                    1
                };
                msgs.extend([(offset, msg)]);
                // Now perform the CaS with the new msgs, if it fails we need to retry a few ops
                let mut cas_res = self
                    .s
                    .cas(
                        Context::new().0,
                        key.clone(),
                        msgs.iter().take(msgs.len() - 1).copied().collect(),
                        msgs,
                        false,
                    )
                    .await;
                while cas_res.is_err() {
                    let mut msgs = self
                        .s
                        .get::<Vec<Pair>>(Context::new().0, key.clone())
                        .await
                        .unwrap();
                    // At this point msgs could be an empty vec
                    debug!("msgs = {:?}", msgs); // To check that the pattern matching is correct
                    let offset = if let Some((last_offset, _)) = msgs.last() {
                        last_offset + 1
                    } else {
                        // If msgs is an empty vec, we need to insert offset 1
                        1
                    };
                    msgs.extend([(offset, msg)]);
                    // Try CaS again, now with the new msgs
                    cas_res = self
                        .s
                        .cas(
                            Context::new().0,
                            key.clone(),
                            msgs.iter().take(msgs.len() - 1).copied().collect(),
                            msgs,
                            false,
                        )
                        .await;
                }
                let resp = ResponseBody::SendOk { offset };
                return runtime.reply(req, resp).await;
            }
            RequestBody::Poll { offsets } => {
                // This one should be easier than Send. Just get the messages present for each key,
                // filter them, and return them. It will inevitably trigger multiple requests to the
                // lin-kv service, but that's fine
                let mut resp_msgs: HashMap<String, Vec<Pair>> = HashMap::new();
                for (key, offset) in offsets.iter() {
                    if let Ok(msgs) = self
                        .s
                        .get::<Vec<Pair>>(Context::new().0, key.to_string())
                        .await
                    {
                        // Same logic as in single-node-kafka from here onwards
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
                // Can we just blindly override the offsets that are currently stored in the server?
                // I think so. Let's go with that.
                self.s
                    .put(Context::new().0, COMMITTED_OFFSETS_KEY.to_string(), offsets)
                    .await
                    .expect("Errors writing committed offsets to lin-kv");
                return runtime.reply_ok(req).await;
            }
            RequestBody::ListCommittedOffsets { keys } => {
                // If there's no committed offsets just return empty
                let offsets = if let Ok(committed_offsets) = self
                    .s
                    .get::<CommittedOffsets>(Context::new().0, COMMITTED_OFFSETS_KEY.to_string())
                    .await
                {
                    // Same logic as in single-node solution
                    committed_offsets
                        .iter()
                        .filter(|(k, _)| keys.contains(k))
                        .map(|(k, v)| (k.clone(), *v))
                        .collect()
                } else {
                    debug!("No committed offsets to return");
                    HashMap::new()
                };
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
