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

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone)]
struct Handler {
    s: Storage,
}

async fn try_main() -> Result<()> {
    // The challenge assignment suggests using the lin-kv service provided by
    // maelstrom. We do need linearizability but only between messages in the same
    // partition (key).
    // It's important to understand the scope of the linearizability guarantee of
    // the lin-kv service. That's not specified in the maelstrom docs so I guess we
    // can assume it's total linearizability?
    // Could we use lin-kv just for the offsets and store the messages locally?
    // And maybe gossip them between nodes? I don't think that would be correct as
    // a client might observe older messages for the first time in more recent polls.
    // Could we do Read + CaS operations on whole lists? This would be correct but
    // feels very brute force. Let's start by trying this just to get the ball rolling.
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
                    // someone else already put something in key, therefore the next read will succeed.
                    // This means we only need to attempt the CaS once and we can ignore the result.
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
                    let msgs = self.s
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
                let mut cas_res = self.s
                    .cas(
                        Context::new().0,
                        key.clone(),
                        msgs.iter().take(msgs.len() - 1).copied().collect(),
                        msgs,
                        false,
                    )
                    .await;
                while cas_res.is_err() {
                    let mut msgs = self.s
                        .get::<Vec<Pair>>(Context::new().0, key.clone())
                        .await
                        .unwrap();
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
                    cas_res = self.s
                        .cas(
                            Context::new().0,
                            key.clone(),
                            msgs.iter().take(msgs.len() - 1).copied().collect(),
                            msgs,
                            false,
                        )
                        .await;
                }
                todo!()
                //     Ok(msgs) => todo!(),
                //     Err(_) => todo!(),
                // }
            }
            RequestBody::Poll { offsets } => {
                todo!()
            }
            RequestBody::CommitOffsets { offsets } => {
                todo!()
            }
            RequestBody::ListCommittedOffsets { keys } => {
                todo!()
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
