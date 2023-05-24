use std::{io::Write, collections::HashMap};

use anyhow::{bail, Context};
use gossip_glomers::{Body, Message, MessageId, Node, Payload, Event};
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

#[derive(Deserialize, Debug)]
struct Topology {
    #[serde(flatten)]
    topology: HashMap<String, Vec<String>>,
}

impl Topology {
    fn get(&self, key: &String) -> Option<&Vec<String>> {
        self.topology.get(key)
    }
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    node_ids: Vec<String>,
    msg_id: MessageId,
    neighbours: Option<Vec<String>>,
    messages: Vec<usize>,
}

impl BroadcastNode {
    fn set_neighbours(&mut self, topology: Topology) -> anyhow::Result<()> {
        let Some(neighbours) = topology.get(&self.node_id) else {
            bail!("node id not found in topology")
        };
        self.neighbours = Some(neighbours.clone());
        eprintln!("{:?}", self);
        Ok(())
    }
}


impl Node<(), BroadcastRequest, BroadcastResponse, ()> for BroadcastNode {
    fn from_init(
        _state: (),
        init: gossip_glomers::Init,
        _: std::sync::mpsc::Sender<Event<BroadcastRequest, BroadcastResponse, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        // we will receive a copy of tx
        // and start a thread that sleeps X time and generates a gossip message 
        Ok(BroadcastNode {
            node_ids: init.node_ids.into_iter().filter(|x| x != &init.node_id).collect(),
            node_id: init.node_id,
            msg_id: 1,
            neighbours: None,
            messages: vec![],
        })
    }

    fn step(
        &mut self,
        msg: Event<BroadcastRequest, BroadcastResponse, ()>,
        output: &mut dyn Write
    ) -> anyhow::Result<()> {
        let Event::Message(msg) = msg else {
            panic!("received unexpected injected variant");
        };
        let request = match msg.body.payload {
            Payload::Request(request) => request,
            Payload::Response(_) => return Ok(()),
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
            BroadcastRequest::Topology { topology } => {
                let topology: Topology =
                    serde_json::from_value(topology).context("Couldn't deserialize topology")?;
                eprintln!("{:?}", topology);
                self.set_neighbours(topology)?;
                Payload::Response(BroadcastResponse::TopologyOk)
            }
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
        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), BroadcastNode, BroadcastRequest, BroadcastResponse, ()>(())
}
