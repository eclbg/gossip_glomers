use std::{
    collections::{HashMap, HashSet},
    io::Write,
    time::Duration,
};

use anyhow::Context;
use gossip_glomers::{Body, Event, Message, MessageId, Node, Payload, GossipingNode};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastRequest {
    Broadcast { message: usize },
    Read,
    Topology { topology: serde_json::Value },
    Gossip { messages: HashSet<usize> },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum BroadcastResponse {
    BroadcastOk,
    ReadOk { messages: HashSet<usize> },
    TopologyOk,
}

#[derive(Debug)]
struct BroadcastNode {
    node_id: String,
    msg_id: MessageId,
    neighbours: Option<Vec<String>>,
    messages: HashSet<usize>,
    known_by_neighbours: HashMap<String, HashSet<usize>>,
}

impl GossipingNode<BroadcastRequest, BroadcastResponse> for BroadcastNode {
    fn gossip(&self, neighbours: &Vec<String>, output: &mut dyn Write) -> Result<(), anyhow::Error> {
        for node_id in neighbours {
            let known_by_node = &self.known_by_neighbours[node_id];
            let message: Message<BroadcastRequest, BroadcastResponse> = Message {
                src: self.node_id.clone(),
                dest: node_id.clone(),
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    payload: Payload::Request(BroadcastRequest::Gossip {
                        messages: self
                            .messages
                            .iter()
                            .copied()
                            .filter(|m| !known_by_node.contains(m))
                            .collect(),
                    }),
                },
            };
            serde_json::to_writer(&mut *output, &message)?;
            output.write_all(b"\n")?;
        };
        Ok(())
    }
}

enum Injected {
    Gossip,
}

impl Node<(), BroadcastRequest, BroadcastResponse, Injected> for BroadcastNode {
    fn from_init(
        _state: (),
        init: gossip_glomers::Init,
        tx: std::sync::mpsc::Sender<Event<BroadcastRequest, BroadcastResponse, Injected>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let neighbourhood_size = 5;
        let node_no: usize = init.node_id[1..]
            .parse()
            .context("couldn't parse node number from node_id")?;

        // node is the leader if it's the first in the chunk
        let is_leader = node_no % neighbourhood_size == 0;

        let mut node_ids: Vec<String> = init.node_ids;
        node_ids.sort_by(|a, b| {
            a[1..]
                .parse::<usize>()
                .unwrap()
                .cmp(&b[1..].parse::<usize>().unwrap())
        });

        let neighbourhood = node_ids
            .chunks(neighbourhood_size)
            .nth(node_no / neighbourhood_size)
            .unwrap()
            .to_vec();

        let neighbours: Vec<String> = if is_leader {
            // exclude ourselves. Leader is always first in the chunk
            let mut neighbours: Vec<String> = neighbourhood[1..].to_vec();
            // Add other leaders: first of every other chunk
            neighbours.extend(
                node_ids
                    .chunks(neighbourhood_size)
                    .enumerate()
                    .filter(|(i, _)| *i != node_no / neighbourhood_size)
                    .map(|(_, chunk)| chunk.iter().next().unwrap().clone())
                    .collect::<Vec<String>>()
            );
            neighbours
        } else {
            // the leader is the only neighbour
            neighbourhood[0..1].to_vec()
        };

        let node = BroadcastNode {
            node_id: init.node_id,
            msg_id: 1,
            neighbours: Some(neighbours.clone()),
            known_by_neighbours: neighbours
                .into_iter()
                .map(|nid| (nid, HashSet::new()))
                .collect(),
            messages: HashSet::new(),
        };

        std::thread::spawn(move || {
            loop {
                // Send a signal to send a Gossip message every 100
                std::thread::sleep(Duration::from_millis(100));
                if tx.send(Event::Injected(Injected::Gossip)).is_err() {
                    break;
                }
            }
        });
        Ok(node)
    }

    fn step(
        &mut self,
        msg: Event<BroadcastRequest, BroadcastResponse, Injected>,
        output: &mut dyn Write,
    ) -> anyhow::Result<()> {
        let msg = match msg {
            Event::Message(msg) => msg,
            Event::Injected(_) => {
                // What should we do when Gossip is received?
                // We need to send the actual Gossip message to our neighbours
                if let Some(neighbours) = &self.neighbours {
                    self.gossip(neighbours, output).context("error gossiping")?;
                }
                // kinda ugly returning like this
                return Ok(());
            }
        };
        let Payload::Request(request) = msg.body.payload else {
            return Ok(())
        };
        let reply_payload = match request {
            BroadcastRequest::Broadcast { message } => {
                self.messages.insert(message);
                Some(Payload::Response(BroadcastResponse::BroadcastOk))
            }
            BroadcastRequest::Read => Some(Payload::Response(BroadcastResponse::ReadOk {
                messages: self.messages.clone(),
            })),
            BroadcastRequest::Topology { .. } => {
                // We ignore the provided topology
                Some(Payload::Response(BroadcastResponse::TopologyOk))
            }
            BroadcastRequest::Gossip { messages } => {
                self.known_by_neighbours
                    .get_mut(&msg.src)
                    .expect("Received gossip from unknown node")
                    .extend(messages.iter().copied());
                self.messages.extend(messages);
                // We don't reply to Gossip messages
                None
            }
        };
        if let Some(reply_payload) = reply_payload {
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
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<(), BroadcastNode, _, _, _>(())
}
