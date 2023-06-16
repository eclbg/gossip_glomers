use async_trait::async_trait;
use log::debug;
use maelstrom::kv::{lin_kv, seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_context::context::Context;

type Pair = (usize, usize);
type CommittedOffsets = HashMap<String, usize>;

static COMMITTED_OFFSETS_KEY: &str = "committed_offsets";

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone)]
struct Handler {
    lin_kv_store: Storage,
    seq_kv_store: Storage,
    // op_id is purely for making logs more meaningful
    op_id: Arc<Mutex<usize>>
}

impl Handler {
    fn get_op_id(&self) -> usize {
        let mut op_id = self.op_id.lock().unwrap();
        let curr_op_id = op_id.clone();
        *op_id += 1;
        return curr_op_id
    }
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
        lin_kv_store: lin_kv(runtime.clone()),
        seq_kv_store: seq_kv(runtime.clone()),
        op_id: Arc::new(Mutex::new(0))
    });
    runtime.with_handler(handler).run().await
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: RequestBody = req.body.as_obj().expect("Error deserializing message body");
        match body {
            RequestBody::Send { key, msg } => {
                let op_id = self.get_op_id();
                debug!("op_id: {:?} Start", op_id);
                // Read from lin-kv and, if the key is empty,
                // try to write with a CaS operation until it succeeds
                //
                // This could be Err if the key doesn't exist
                let read_res = self.lin_kv_store.get::<Vec<Pair>>(Context::new().0, key.clone()).await;
                let mut msgs;
                if read_res.is_err() {
                    // If CaS succeeds the next get will surely succeed. If CaS fails it's because
                    // someone else already put something in key, therefore the next read will
                    // succeed. This means we only need to attempt the CaS once and we can ignore
                    // the result.
                    debug!("op_id: {:?} Getting here should mean that there's no messges in key: {}", op_id, key);
                    let _ = self
                        .lin_kv_store
                        .cas(
                            Context::new().0,
                            key.clone(),
                            Vec::<Pair>::new(),
                            Vec::<Pair>::new(),
                            true,
                        )
                        .await;
                    msgs = self
                        .lin_kv_store
                        .get::<Vec<Pair>>(Context::new().0, key.clone())
                        .await
                        .unwrap();
                    debug!("op_id: {:?} Key: {} should now be inited", op_id, key);
                } else {
                    msgs = read_res.unwrap()
                };
                // At this point msgs could be an empty vec
                debug!("op_id: {:?} msgs: {:?}", op_id, msgs); // To check that the pattern matching is correct
                let mut offset = if let Some((last_offset, _)) = msgs.last() {
                    last_offset + 1
                } else {
                    // If msgs is an empty vec, we need to insert offset 1
                    1
                };
                msgs.extend([(offset, msg)]);
                // Now perform the CaS with the new msgs, if it fails we need to retry a few ops
                debug!("op_id: {:?} Trying to insert msgs: {:?}", op_id, msgs);
                let mut cas_res = self
                    .lin_kv_store
                    .cas(
                        Context::new().0,
                        key.clone(),
                        msgs.iter().take(msgs.len() - 1).copied().collect(),
                        msgs,
                        false,
                    )
                    .await;
                while cas_res.is_err() {
                    debug!("op_id: {:?} Insert failed. Someone else must have written to key: {}", op_id, key);
                    msgs = self
                        .lin_kv_store
                        .get::<Vec<Pair>>(Context::new().0, key.clone())
                        .await
                        .unwrap();
                    // At this point msgs could be an empty vec
                    debug!("op_id: {:?} msgs: {:?}", op_id, msgs);
                    offset = if let Some((last_offset, _)) = msgs.last() {
                        last_offset + 1
                    } else {
                        // If msgs is an empty vec, we need to insert offset 1
                        1
                    };
                    msgs.extend([(offset, msg)]);
                    // Try CaS again, now with the new msgs
                    debug!("op_id: {:?} Trying to insert msgs: {:?}", op_id, msgs);
                    cas_res = self
                        .lin_kv_store
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
                debug!("op_id: {:?} Done", op_id);
                return runtime.reply(req, resp).await;
            }
            RequestBody::Poll { offsets } => {
                let op_id = self.get_op_id();
                debug!("op_id: {:?} Start", op_id);
                // This one should be easier than Send. Just get the messages present for each key,
                // filter them, and return them. It will inevitably trigger multiple requests to the
                // lin-kv service, but that's fine
                let mut resp_msgs: HashMap<String, Vec<Pair>> = HashMap::new();
                for (key, offset) in offsets.iter() {
                    if let Ok(msgs) = self
                        .lin_kv_store
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
                            debug!("op_id: {:?} No messages on or after offset", op_id)
                        }
                    } else {
                        debug!("op_id: {:?} Polled for non-existing key {}", op_id, key)
                    }
                }
                let resp = ResponseBody::PollOk { msgs: resp_msgs };
                debug!("op_id: {:?} Done", op_id);
                return runtime.reply(req, resp).await;
            }
            RequestBody::CommitOffsets { offsets } => {
                let op_id = self.get_op_id();
                debug!("op_id: {:?} Start", op_id);
                // Can we just blindly override the offsets that are currently stored in the server?
                // I think so. Let's go with that.
                self.seq_kv_store
                    .put(Context::new().0, COMMITTED_OFFSETS_KEY.to_string(), offsets)
                    .await
                    .expect("Errors writing committed offsets to lin-kv");
                debug!("op_id: {:?} Done", op_id);
                return runtime.reply_ok(req).await;
            }
            RequestBody::ListCommittedOffsets { keys } => {
                let op_id = self.get_op_id();
                debug!("op_id: {:?} Start", op_id);
                // If there's no committed offsets just return empty
                let offsets = if let Ok(committed_offsets) = self
                    .seq_kv_store
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
                    debug!("op_id: {:?} No committed offsets to return", op_id);
                    HashMap::new()
                };
                let resp = ResponseBody::ListCommittedOffsetsOk { offsets };
                debug!("op_id: {:?} Done", op_id);
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
