use std::io::{Write, StdoutLock};

use serde::{Deserialize, Serialize};
use anyhow::{self, Context, bail};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Payload {
    Request(Request),
    Response(Response),
}

impl Payload {
    fn request(self) -> Option<Request> {
        match self {
            Payload::Request(request) => Some(request),
            Payload::Response(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Request {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: serde_json::Value
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Response {
    InitOk,
    EchoOk {
        echo: serde_json::Value
    }
}

#[derive(Debug, Default)]
pub struct EchoNode {
    name: Option<String>,
    msg_id: usize,
}

impl EchoNode {
    pub fn init(msg: &Message) -> anyhow::Result<EchoNode> {
        let payload = &msg.body.payload;
        match payload {
            Payload::Request(Request::Init { node_id, .. }) => Ok(EchoNode { name: Some(node_id.clone()), ..EchoNode::default() }),
            _ => bail!("Tried to initialise node with wrong payload variant"),
        }
    }

    fn create_reply(&mut self, msg: Message) -> anyhow::Result<Message> {
        let reply: Message;
        let request = msg.body.payload.request().ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        match request {
            Request::Init { node_id, .. } => {
                self.name = Some(node_id);
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::InitOk)
                    }
                };
            }
            Request::Echo { echo } => {
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id) ,
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::EchoOk { echo })
                    }
                };
            }
        }
        self.msg_id += 1;
        Ok(reply)
    }

    pub fn reply(&mut self, msg: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        let reply = self.create_reply(msg)?;
        serde_json::to_writer(&mut *output, &reply).context("Writing to stdout")?;
        output.write_all(b"\n").context("Writing newline to stdout")?;
        Ok(())
    }
}
