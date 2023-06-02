use async_trait::async_trait;
use log::info;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::default());
    runtime.with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    set: Arc<Mutex<std::collections::HashSet<u64>>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: Result<RequestBody> = req.body.as_obj();
        match body {
            Ok(RequestBody::Broadcast { message }) => {
                self.set.lock().unwrap().insert(message.clone());
                return runtime.reply_ok(req).await;
            }
            Ok(RequestBody::Read) => {
                let resp = ResponseBody::ReadOk {
                    messages: Vec::from_iter(self.set.lock().unwrap().iter().copied()),
                };
                return runtime.reply(req, resp).await;
            }
            Ok(RequestBody::Topology { .. }) => {
                return runtime.reply_ok(req).await;
            }
            Ok(RequestBody::Init { .. }) => {
                // spawn into tokio (instead of runtime) to not to wait
                // until it is completed, as it will never be.
                let (r0, h0) = (runtime.clone(), self.clone());
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        info!("emit replication signal");
                        let s = h0.set.lock().unwrap();
                        for n in r0.neighbours() {
                            let msg = RequestBody::Gossip {
                                messages: to_seq(&s),
                            };
                            drop(r0.send_async(n, msg));
                        }
                    }
                });
                return Ok(());
            }
            Ok(RequestBody::Gossip { messages }) => {
                self.set.lock().unwrap().extend(messages.clone());
                return Ok(());
            }
            _ => done(runtime, req),
        }
    }
}

fn to_seq(s: &MutexGuard<HashSet<u64>>) -> Vec<u64> {
    s.iter().copied().collect()
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: std::collections::HashMap<String, Vec<String>>,
    },
    Gossip {
        messages: Vec<u64>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    ReadOk { messages: Vec<u64> },
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_read_ok() {
        let body = ResponseBody::ReadOk {
            messages: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(
            "{\"type\":\"read_ok\",\"messages\":[1,2,3,4,5]}",
            serde_json::to_string::<ResponseBody>(&body).unwrap()
        )
    }
}
