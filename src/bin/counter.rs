use async_trait::async_trait;
use maelstrom::kv::{seq_kv, Storage, KV};
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_context::context::Context;

static COUNTER_KEY: &str = "counter";

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone)]
struct Handler {
    s: Storage,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: Result<RequestBody> = req.body.as_obj();
        match body {
            Ok(RequestBody::Add { delta }) => {
                let curr_counter_value = self
                    .s
                    .get::<u64>(Context::new().0, String::from(COUNTER_KEY))
                    .await
                    .unwrap();
                let cas_result = self
                    .s
                    .cas(
                        Context::new().0,
                        String::from(COUNTER_KEY),
                        curr_counter_value,
                        curr_counter_value + delta,
                        false,
                    )
                    .await;
                while let Err(..) = cas_result {
                    let curr_counter_value = self
                        .s
                        .get::<u64>(Context::new().0, String::from(COUNTER_KEY))
                        .await
                        .unwrap();
                    let cas_result = self.s.cas(
                        Context::new().0,
                        String::from(COUNTER_KEY),
                        curr_counter_value,
                        curr_counter_value + delta,
                        false,
                    );
                }
                return runtime.reply_ok(req).await;
            }
            Ok(RequestBody::Read) => {
                let curr_counter_value = self
                    .s
                    .get::<u64>(Context::new().0, String::from(COUNTER_KEY))
                    .await
                    .unwrap();
                return runtime
                    .reply(
                        req,
                        ResponseBody::ReadOk {
                            value: curr_counter_value,
                        },
                    )
                    .await;
            }
            Ok(RequestBody::Init { .. }) => {
                // Set counter to 0 at the start. If another node has already set it to 0
                // don't retry or anything
                let _ = self
                    .s
                    .cas(Context::new().0, String::from(COUNTER_KEY), 0, 0, true)
                    .await;
                Ok(())
            }
            _ => done(runtime, req),
        }
    }
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler {
        s: seq_kv(runtime.clone()),
    });
    runtime.with_handler(handler).run().await
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Add {
        delta: u64,
    },
    Read,
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    ReadOk { value: u64 },
}
