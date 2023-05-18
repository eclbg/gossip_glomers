use std::io::{StdoutLock, Write};

use gossip_glomers::{Body, Message, Node, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum EchoRequest {
    Echo { echo: serde_json::Value },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum EchoResponse {
    EchoOk { echo: serde_json::Value },
}

#[derive(Debug)]
struct EchoNode {
    msg_id: usize,
}

impl Node<(), EchoRequest, EchoResponse> for EchoNode {
    fn from_init(_state: (), _init: gossip_glomers::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { msg_id: 1 })
    }

    fn step(
        &mut self,
        msg: Message<EchoRequest, EchoResponse>,
        output: &mut StdoutLock
    ) -> anyhow::Result<()> {
        let request = msg
            .body
            .payload
            .request()
            .ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        let EchoRequest::Echo { echo } = request;
        let reply: Message<EchoRequest, EchoResponse> = Message {
            src: msg.dest,
            dest: msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: msg.body.msg_id,
                payload: Payload::Response(EchoResponse::EchoOk { echo }),
            },
        };
        self.msg_id += 1;
        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<_, EchoNode, _, _>(())
}
