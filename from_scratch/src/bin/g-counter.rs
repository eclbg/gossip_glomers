use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
    time::Duration,
};

use anyhow::Context;
use gossip_glomers::{Body, Event, Message, Node, Payload};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum GOCounterRequest {
    Add { delta: usize },
    Read,
    // #[serde(rename = "read_ok")]
    // KVReadOk,
    // #[serde(rename = "error")]
    // KVError
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum GOCounterResponse {
    AddOk,
    ReadOk { value: usize },
}

#[derive(Debug)]
struct GOCounterNode {
    node_id: String,
}

impl Node<(), GOCounterRequest, GOCounterResponse> for GOCounterNode {
    fn from_init(
        _state: (),
        init: gossip_glomers::Init,
        _tx: std::sync::mpsc::Sender<Event<GOCounterRequest, GOCounterResponse, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = GOCounterNode {
            node_id: init.node_id,
        };
        Ok(node)
    }

    fn step(
        &mut self,
        msg: Event<GOCounterRequest, GOCounterResponse, ()>,
        output: &mut dyn Write,
    ) -> anyhow::Result<()> {
        let kv_read_message = format!(r#"{{"src":"{}","dest":"seq-kv","body":{{"type":"read"}}}}"#, self.node_id);
        output.write_all(kv_read_message.as_bytes())?;
        eprintln!("sent kv read message");
        let msg = match msg {
            Event::Message(msg) => msg,
            Event::Injected(_) => {
                panic!("Received unexpected injected message")
            }
        };
        let Payload::Request(request) = msg.body.payload else {
            return Ok(())
        };
        let reply_payload = match request {
            GOCounterRequest::Add { .. } => {
                None
            }
            GOCounterRequest::Read => {
                // Send read request to the KV thingie
                // but we need the response from the KV service to get back to the client
                // and it will arrive to stdin just as the client calls
                // it feels to me like we do need some kind of state to store the read requests to which we need to reply
                // let message = format!(r#"{{"src":"{}","dest":"seq-kv","body":{{"type":"read"}}}}"#, self.node_id);
                None
            },
        };
        if let Some(reply_payload) = reply_payload {
            let reply: Message<GOCounterRequest, GOCounterResponse> = Message {
                src: self.node_id.clone(),
                dest: msg.src,
                body: Body {
                    msg_id: None,
                    in_reply_to: msg.body.msg_id, //This will not be there for the kv comms
                    payload: reply_payload,
                },
            };
            serde_json::to_writer(&mut *output, &reply)?;
            output.write_all(b"\n")?;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), GOCounterNode, _, _, _>(())
}
