use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use maelstrom::{
    kv::{seq_kv, Storage, KV},
    protocol::Message,
    Node, Result, Runtime,
};
use serde::de;
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use tokio_context::context::Context;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone)]
struct Handler {
    storage: Storage,
}

async fn try_main() -> Result<()> {
    let runtime = Runtime::new();
    let handler = Arc::new(Handler {
        storage: seq_kv(runtime.clone()),
    });
    runtime.with_handler(handler).run().await
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        let body: RequestBody = req.body.as_obj().expect("Error deserializing message body");
        match body {
            RequestBody::Transaction { txn } => {
                let mut ops: Vec<Operation> =
                    txn.into_iter().map(|so| so.try_into().unwrap()).collect();
                debug!("{:?}", ops);
                for op in ops.iter_mut() {
                    match op {
                        Operation::Read { key, value } => {
                            let read_res = self
                                .storage
                                .get(Context::new().0, key.to_string())
                                .await;
                            *value = match read_res {
                                Ok(val) => Some(val),
                                Err(_) => None,
                            };
                        }
                        Operation::Write { key, value } => self
                            .storage
                            .put(Context::new().0, key.to_string(), value)
                            .await
                            .unwrap(),
                    }
                }
                debug!("{:?}", ops);
                return runtime
                    .reply(req.clone(), ResponseBody::TransactionOk { txn: ops })
                    .await;
            }
            RequestBody::Init { .. } => Ok(()),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename = "txn")]
    Transaction { txn: Vec<Operation> },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    #[serde(rename = "txn_ok")]
    TransactionOk { txn: Vec<Operation> },
}

#[derive(Debug, PartialEq)]
enum Operation {
    Read { key: usize, value: Option<usize> },
    Write { key: usize, value: usize },
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
            Operation::Write {
                key: from_key,
                value: to_key,
            } => {
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
        #[derive(Deserialize)]
        struct InnerOperation(char, usize, Option<usize>);

        let inner = InnerOperation::deserialize(deserializer)?;
        match inner.0 {
            'r' => Ok(Operation::Read {
                key: inner.1,
                value: inner.2,
            }),
            'w' => Ok(Operation::Write {
                key: inner.1,
                value: inner.2.expect("must be a value"),
            }),
            x => Err(de::Error::custom(format!(
                "found unexpected operation type {}",
                x
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_operation() {
        let read_resp = Operation::Read {
            key: 15,
            value: Some(10),
        };
        assert_eq!(r#"["r",15,10]"#, serde_json::to_string(&read_resp).unwrap());
        let read_req = Operation::Read {
            key: 15,
            value: None,
        };
        assert_eq!(
            r#"["r",15,null]"#,
            serde_json::to_string(&read_req).unwrap()
        );
        let write = Operation::Write { key: 7, value: 12 };
        assert_eq!(r#"["w",7,12]"#, serde_json::to_string(&write).unwrap());
    }

    #[test]
    fn deserialize_operation() {
        let read_resp = Operation::Read {
            key: 15,
            value: Some(10),
        };
        let raw = r#"["r",15,10]"#;
        assert_eq!(read_resp, serde_json::from_str::<Operation>(&raw).unwrap());
        let read_req = Operation::Read {
            key: 15,
            value: None,
        };
        let raw = r#"["r",15,null]"#;
        assert_eq!(read_req, serde_json::from_str::<Operation>(&raw).unwrap());
        let write = Operation::Write { key: 7, value: 12 };
        let raw = r#"["w",7,12]"#;
        assert_eq!(write, serde_json::from_str::<Operation>(&raw).unwrap());
    }
}
