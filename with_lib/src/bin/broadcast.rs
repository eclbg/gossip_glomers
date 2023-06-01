use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone, Default, Debug)]
struct Inner {
    set: std::collections::HashSet<u64>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: Result<RequestBody> = req.body.as_obj();
        match body {
            Ok(RequestBody::Broadcast { message }) => {
                {
                    //I don't fully understand why inner has to be dropped before
                    //returning a Future
                    let mut inner = self.inner.lock().unwrap();
                    inner.set.insert(message.clone());
                }
                let resp = ResponseBody::BroadcastOk;
                return runtime.reply(req, resp).await;
            }
            Ok(RequestBody::Read) => {
                let messages = self.inner.lock().unwrap().set.clone();
                let resp = ResponseBody::ReadOk {
                    messages: Vec::from_iter(messages),
                };
                return runtime.reply(req, resp).await;
            }
            Ok(RequestBody::Topology { .. }) => {
                return runtime.reply_ok(req).await;
            }
            _ => done(runtime, req),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Broadcast {
        message: u64,
    },
    Read,
    Topology {
        topology: std::collections::HashMap<String, Vec<String>>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    BroadcastOk,
    ReadOk { messages: Vec<u64> },
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_broadcast_ok() {
        let body = ResponseBody::BroadcastOk;
        assert_eq!(
            "{\"type\":\"broadcast_ok\"}",
            serde_json::to_string::<ResponseBody>(&body).unwrap()
        )
    }

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
