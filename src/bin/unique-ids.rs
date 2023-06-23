use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::Serialize;
use std::sync::Arc;
use ulid;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Clone, Default)]
struct Handler {}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if req.get_type() == "generate" {
            let response = GenerateOk {
                guid: ulid::Ulid::new().to_string(),
            };
            return runtime.reply(req.clone(), response).await;
        }

        done(runtime, req)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
#[serde(rename = "generate_ok")]
struct GenerateOk {
    #[serde(rename = "id")]
    guid: String,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_generate_ok() {
        let body = GenerateOk {
            guid: String::from("1"),
        };
        assert_eq!(
            r#"{"type":"generate_ok","id":"1"}"#,
            serde_json::to_string::<GenerateOk>(&body).unwrap()
        )
    }
}
