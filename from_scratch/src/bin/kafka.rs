use std::{collections::HashMap, io::Write};

use gossip_glomers::{Body, Event, Message, MessageId, Node, Payload};
use serde::{Deserialize, Serialize};
use log::debug;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum KafkaRequest {
    Send { key: String, msg: usize },
    Poll { offsets: HashMap<String, usize> },
    CommitOffsets { offsets: HashMap<String, usize> },
    ListCommittedOffsets { keys: Vec<String> },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum KafkaResponse {
    SendOk {
        offset: usize,
    },
    PollOk {
        msgs: HashMap<String, Vec<[usize; 2]>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Debug)]
struct KafkaNode {
    node_id: String,
    msg_id: MessageId,
    logs: HashMap<String, Vec<usize>>,
    committed_offsets: HashMap<String, usize>,
}

impl Node<(), KafkaRequest, KafkaResponse> for KafkaNode {
    fn from_init(
        _state: (),
        init: gossip_glomers::Init,
        _: std::sync::mpsc::Sender<Event<KafkaRequest, KafkaResponse, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(KafkaNode {
            node_id: init.node_id,
            msg_id: 1,
            logs: HashMap::new(),
            committed_offsets: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        msg: Event<KafkaRequest, KafkaResponse, ()>,
        output: &mut dyn Write,
    ) -> anyhow::Result<()> {
        let Event::Message(msg) = msg else {
            panic!("received unexpected injected variant");
        };
        let request = msg
            .body
            .payload
            .request()
            .ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        let reply_payload = match request {
            KafkaRequest::Send { key, msg } => {
                debug!("Received send request. key: {}, msg: {}", key, msg);
                self.logs
                    .entry(key.clone())
                    .or_insert_with(|| Vec::new())
                    .push(msg);
                debug!("Currently in logs: {:?}", self.logs);
                let offset = self.logs.get(&key).unwrap().len() - 1;
                debug!("Will send back offset: {}", offset);
                Payload::Response(KafkaResponse::SendOk { offset })
            }
            KafkaRequest::Poll { offsets } => {
                debug!("Received poll message. offsets = {:?}", offsets);
                let mut msgs: HashMap<String, Vec<[usize; 2]>> = HashMap::new();
                debug!("Currently in logs: {:?}", self.logs);
                if offsets.is_empty() {
                    for (key, messages) in self.logs.iter() {
                        let offset = 0;
                        let mut pairs: Vec<[usize; 2]> = Vec::new();
                        for (i, msg) in messages.iter().enumerate() {
                            pairs.push([*msg, offset + i]);
                        }
                        msgs.insert(key.clone(), pairs);
                    }
                } else {
                    for (key, offset) in offsets.iter() {
                        let mut pairs: Vec<[usize; 2]> = Vec::new();
                        if let Some(msgs2) = self.logs.get(key) {
                            for (i, &msg) in msgs2.iter().skip(*offset).enumerate() {
                                pairs.push([msg, offset + i]);
                            }
                            msgs.insert(key.clone(), pairs);
                        } else {
                            debug!("Polled for non-existing key {}", key)
                        }
                    }
                }
                debug!("Will send back messages: {:?}", msgs);
                Payload::Response(KafkaResponse::PollOk { msgs })
            }
            KafkaRequest::CommitOffsets { offsets } => {
                debug!("Received commit_offsets. Offsets: {:?}", offsets);
                debug!("Committed offsets before processing: {:?}", self.committed_offsets);
                self.committed_offsets.extend(offsets);
                debug!("Committed offsets after processing: {:?}", self.committed_offsets);
                let resp = Payload::Response(KafkaResponse::CommitOffsetsOk);
                debug!("Response: {:?}", serde_json::to_string(&resp));
                resp
            }
            KafkaRequest::ListCommittedOffsets { keys } => {
                debug!("Received list_commit_offsets. keys: {:?}", keys);
                let offsets = self
                    .committed_offsets
                    .iter()
                    .filter(|(k, _)| keys.contains(k))
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();
                debug!("Will reply with offsets: {:?}", offsets);
                let resp = Payload::Response(KafkaResponse::ListCommittedOffsetsOk { offsets });
                debug!("Response: {:?}", serde_json::to_string(&resp));
                resp
            }
        };

        self.msg_id += 1;
        let reply: Message<KafkaRequest, KafkaResponse> = Message {
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
    gossip_glomers::run::<_, KafkaNode, _, _, _>(())
}
