use std::io::Write;

use gossip_glomers::{Body, Message, Node, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastRequest {
    Broadcast { message: usize },
    BroadcastEcho { message: usize },
    Read,
    Topology { topology: serde_json::Value },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastResponse {
    BroadcastOk,
    ReadOk { messages: Vec<usize> },
    TopologyOk,
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    node_ids: Vec<String>,
    msg_id: usize,
    messages: Vec<usize>,
}

impl Node<(), BroadcastRequest, BroadcastResponse> for BroadcastNode {
    fn from_init(_state: (), init: gossip_glomers::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(BroadcastNode {
            node_id: init.node_id,
            node_ids: init.node_ids,
            msg_id: 1,
            messages: vec![],
        })
    }

    fn create_reply(
        &mut self,
        msg: Message<BroadcastRequest, BroadcastResponse>,
    ) -> anyhow::Result<Option<Message<BroadcastRequest, BroadcastResponse>>> {
        eprintln!("{:?}", self);
        let request = match msg.body.payload {
            Payload::Request(request) => request,
            Payload::Response(_) => return Ok(None),
        };
        let reply_payload = match request {
            BroadcastRequest::Broadcast { message } => {
                self.messages.push(message);
                for node_id in &self.node_ids {
                    let mut stdout = std::io::stdout().lock();
                    let message: Message<BroadcastRequest, BroadcastResponse> = Message {
                        src: self.node_id.clone(),
                        dest: node_id.clone(),
                        body: Body {
                            msg_id: Some(self.msg_id),
                            in_reply_to: None,
                            payload: Payload::Request(BroadcastRequest::BroadcastEcho { message }),
                        },
                    };
                    serde_json::to_writer(&mut stdout, &message)?;
                    stdout.write_all(b"\n")?;
                }
                Payload::Response(BroadcastResponse::BroadcastOk)
            }
            BroadcastRequest::Read => Payload::Response(BroadcastResponse::ReadOk {
                messages: self.messages.clone(),
            }),
            BroadcastRequest::Topology { .. } => Payload::Response(BroadcastResponse::TopologyOk),
            BroadcastRequest::BroadcastEcho { message } => {
                self.messages.push(message);
                Payload::Response(BroadcastResponse::BroadcastOk)
            }
        };
        self.msg_id += 1;
        let reply: Message<BroadcastRequest, BroadcastResponse> = Message {
            src: self.node_id.clone(),
            dest: msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: msg.body.msg_id,
                payload: reply_payload,
            },
        };
        Ok(Some(reply))
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), BroadcastNode, BroadcastRequest, BroadcastResponse>(())
}
