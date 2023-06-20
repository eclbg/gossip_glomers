use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use log::debug;
use maelstrom::{protocol::Message, Node, Result, Runtime};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

#[derive(Clone, Default)]
struct Handler {
    storage: Arc<Mutex<HashMap<usize, usize>>>,
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
            RequestBody::Transaction { txn } => {
                let mut ops: Vec<Operation> =
                    txn.into_iter().map(|so| so.try_into().unwrap()).collect();
                debug!("{:?}", ops);
                for op in ops.iter_mut() {
                    match op {
                        Operation::Read { key, value } => {
                            *value = self.storage.lock().unwrap().get(&key).copied();
                        }
                        Operation::Write { key, value } => {
                            let mut s = self.storage.lock().unwrap();
                            s.entry(key.clone()).or_insert_with(|| value.clone());
                        }
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
    Transaction { txn: Vec<SerdeOperation> },
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

#[derive(Debug, Deserialize, Serialize)]
struct SerdeOperation(char, usize, Option<usize>);

impl TryInto<Operation> for SerdeOperation {
    fn try_into(self) -> std::result::Result<Operation, Self::Error> {
        let type_ = self.0;
        match type_ {
            'r' => Ok(Operation::Read {
                key: self.1,
                value: self.2,
            }),
            'w' => Ok(Operation::Write {
                key: self.1,
                value: self.2.expect("must be a value"),
            }),
            _ => Err(()),
        }
    }

    type Error = ();
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
        assert_eq!(
            read_resp,
            serde_json::from_str::<SerdeOperation>(&raw)
                .unwrap()
                .try_into()
                .unwrap()
        );
        let read_req = Operation::Read {
            key: 15,
            value: None,
        };
        let raw = r#"["r",15,null]"#;
        assert_eq!(
            read_req,
            serde_json::from_str::<SerdeOperation>(&raw)
                .unwrap()
                .try_into()
                .unwrap()
        );
        let write = Operation::Write { key: 7, value: 12 };
        let raw = r#"["w",7,12]"#;
        assert_eq!(
            write,
            serde_json::from_str::<SerdeOperation>(&raw)
                .unwrap()
                .try_into()
                .unwrap()
        );
    }
}
