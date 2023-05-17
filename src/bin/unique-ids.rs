use gossip_glomers::{Body, Message, Node, Payload};
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
        guid: String
    },
}

#[derive(Debug)]
struct UniqueIDNode {
    node_id: String,
    msg_id: usize,
}

impl Node<(), UniqueIDRequest, UniqueIDResponse> for UniqueIDNode {
    fn from_init(_state: (), init: gossip_glomers::Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(
            UniqueIDNode {
                node_id: init.node_id,
                msg_id: 1 
            }
        )
    }

    fn create_reply(
        &mut self,
        msg: Message<UniqueIDRequest, UniqueIDResponse>,
    ) -> anyhow::Result<Option<Message<UniqueIDRequest, UniqueIDResponse>>> {
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
        Ok(Some(reply))
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), UniqueIDNode, UniqueIDRequest, UniqueIDResponse>(())
}
