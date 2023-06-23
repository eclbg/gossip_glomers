use std::io::Write;

use gossip_glomers::{Body, Message, MessageId, Node, Payload, Event};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum UniqueIDRequest {
    Generate,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum UniqueIDResponse {
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

#[derive(Debug)]
struct UniqueIDNode {
    node_id: String,
    msg_id: MessageId,
}

impl Node<(), UniqueIDRequest, UniqueIDResponse> for UniqueIDNode {
    fn from_init(
        _state: (),
        init: gossip_glomers::Init,
        _: std::sync::mpsc::Sender<Event<UniqueIDRequest, UniqueIDResponse, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIDNode {
            node_id: init.node_id,
            msg_id: 1,
        })
    }

    fn step(
        &mut self,
        msg: Event<UniqueIDRequest, UniqueIDResponse, ()>,
        output: &mut dyn Write,
    ) -> anyhow::Result<()> {
        let Event::Message(msg) = msg else {
            panic!("received unexpected injected variant");
        };
        let guid = format!("{}-{}", self.node_id, self.msg_id);
        let reply: Message<UniqueIDRequest, UniqueIDResponse> = Message {
            src: self.node_id.clone(),
            dest: msg.src,
            body: Body {
                msg_id: Some(self.msg_id),
                in_reply_to: msg.body.msg_id,
                payload: Payload::Response(UniqueIDResponse::GenerateOk { guid }),
            },
        };
        self.msg_id += 1;
        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), UniqueIDNode, _, _, _>(())
}
