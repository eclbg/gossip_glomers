use gossip_glomers::{Body, Message, Node, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastRequest {
    Broadcast { message: usize },
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
            msg_id: 1,
            messages: vec![],
        })
    }

    fn create_reply(
        &mut self,
        msg: Message<BroadcastRequest, BroadcastResponse>,
    ) -> anyhow::Result<Message<BroadcastRequest, BroadcastResponse>> {
        let request = msg
            .body
            .payload
            .request()
            .expect("message received is not a request");
        let reply_payload = match request {
            BroadcastRequest::Broadcast { message } => {
                self.messages.push(message);
                Payload::Response(BroadcastResponse::BroadcastOk)
            }
            BroadcastRequest::Read => {
                Payload::Response(BroadcastResponse::ReadOk { messages: self.messages.clone() })
            }
            BroadcastRequest::Topology { .. } => Payload::Response(BroadcastResponse::TopologyOk),
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
        Ok(reply)
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), BroadcastNode, BroadcastRequest, BroadcastResponse>(())
}
