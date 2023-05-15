use std::io::StdoutLock;

use anyhow::{self, bail, Context};
use echo::{Body, Message, Node, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Request {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: serde_json::Value,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Response {
    InitOk,
    EchoOk { echo: serde_json::Value },
}

#[derive(Debug)]
pub struct EchoNode {
    name: Option<String>,
    msg_id: usize,
}

impl Node<Request, Response> for EchoNode {
    fn init(
        &mut self,
        msg: Message<Request, Response>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let payload = &msg.body.payload;
        match &payload {
            Payload::Request(Request::Init { node_id, .. }) => {
                self.name = Some(node_id.clone());
                self.reply(msg, output)
                    .context("Failed to reply to init message")?;
                Ok(())
            }
            _ => bail!("Tried to initialise node with wrong payload variant"),
        }
    }

    fn create_reply(
        &mut self,
        msg: Message<Request, Response>,
    ) -> anyhow::Result<Message<Request, Response>> {
        let reply: Message<Request, Response>;
        let request = msg
            .body
            .payload
            .request()
            .ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        match request {
            Request::Init { node_id, .. } => {
                self.name = Some(node_id);
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::InitOk),
                    },
                };
            }
            Request::Echo { echo } => {
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::EchoOk { echo }),
                    },
                };
            }
        }
        self.msg_id += 1;
        Ok(reply)
    }
}

fn main() -> anyhow::Result<()> {
    let node = EchoNode {
        name: None,
        msg_id: 0,
    };
    echo::run(node)
}
