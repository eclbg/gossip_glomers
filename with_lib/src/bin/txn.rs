use std::sync::Arc;

use async_trait::async_trait;
use maelstrom::{protocol::Message, Node, Result, Runtime};
use serde::{Deserialize, Serialize, ser::SerializeSeq};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone, Default)]
struct Handler {}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler::default());
    runtime.with_handler(handler).run().await
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        todo!()
    }
}

// {
//   "type": "txn",
//   "msg_id": 3,
//   "txn": [
//     ["r", 1, null],
//     ["w", 1, 6],
//     ["w", 2, 9]
//   ]
// }
#[derive(Deserialize, Serialize, Debug)]
struct MessageBody {
    txn: Vec<Operation>,
}

#[derive(Debug)]
enum Operation {
    Read { key: usize, value: Option<usize> },
    Write { from_key: usize, to_key: usize },
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match self {
            Operation::Read { key, value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            Operation::Write { from_key, to_key } => {
                seq.serialize_element("w")?;
                seq.serialize_element(from_key)?;
                seq.serialize_element(to_key)?;
            }
        };
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_operation() {
        let read_resp = Operation::Read { key: 15, value: Some(10) };
        assert_eq!(r#"["r",15,10]"#, serde_json::to_string(&read_resp).unwrap());
        let read_req = Operation::Read { key: 15, value: None };
        assert_eq!(r#"["r",15,null]"#, serde_json::to_string(&read_req).unwrap());
        let write = Operation::Write { from_key: 7, to_key: 12 };
        assert_eq!(r#"["w",7,12]"#, serde_json::to_string(&write).unwrap());
    }
}
