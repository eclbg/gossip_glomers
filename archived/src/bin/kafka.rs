use std::{collections::HashMap, io::Write};

use gossip_glomers::{Body, Event, Message, MessageId, Node, Payload};
use log::{debug, info};
use serde::{Deserialize, Serialize};

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
        msgs: HashMap<String, Vec<(usize, usize)>>,
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
    logs: HashMap<String, Vec<(usize, usize)>>,
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
        info!(
            "Full request: {}",
            serde_json::to_string(&msg).expect("Couldn't serialize request")
        );
        let request = msg
            .body
            .payload
            .request()
            .ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        let reply_payload = match request {
            KafkaRequest::Send { key, msg } => {
                debug!("Received send request. key: {}, msg: {}", key, msg);
                // create entry if not exists
                let msgs = self.logs.entry(key.clone()).or_insert_with(|| Vec::new());
                // len + 1 as we still haven't pushed the new message
                let offset = msgs.len() + 1;
                debug!("Will send back offset: {}", offset);
                msgs.push((offset, msg));
                debug!("Currently in logs: {:?}", self.logs);
                let resp = Payload::Response(KafkaResponse::SendOk { offset });
                debug!("Response: {}", serde_json::to_string(&resp).unwrap());
                resp
            }
            KafkaRequest::Poll { offsets } => {
                debug!("Received poll message. offsets = {:?}", offsets);
                let mut resp_msgs: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
                debug!("Currently in logs: {:?}", self.logs);
                for (key, offset) in offsets.iter() {
                    if let Some(msgs) = self.logs.get(key) {
                        if let Some((first_to_return_idx, _)) = msgs
                            .iter()
                            .enumerate()
                            .filter(|(_, (o, _))| o >= offset)
                            .next()
                        {
                            resp_msgs.insert(
                                key.clone(),
                                msgs.iter().skip(first_to_return_idx).copied().collect(),
                            );
                        } else {
                            debug!("No messages on or after offset")
                        }
                    } else {
                        debug!("Polled for non-existing key {}", key)
                    }
                }
                let resp = Payload::Response(KafkaResponse::PollOk { msgs: resp_msgs });
                debug!("Response: {}", serde_json::to_string(&resp).unwrap());
                resp
            }
            KafkaRequest::CommitOffsets { offsets } => {
                debug!("Received commit_offsets. Offsets: {:?}", offsets);
                debug!(
                    "Committed offsets before processing: {:?}",
                    self.committed_offsets
                );
                self.committed_offsets.extend(offsets);
                debug!(
                    "Committed offsets after processing: {:?}",
                    self.committed_offsets
                );
                let resp = Payload::Response(KafkaResponse::CommitOffsetsOk);
                debug!("Response: {}", serde_json::to_string(&resp).unwrap());
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
                debug!("Response: {}", serde_json::to_string(&resp).unwrap());
                resp
            }
        };

        self.msg_id += 1;
        let reply: Message<KafkaRequest, KafkaResponse> = Message {
            src: self.node_id.clone(),
            dest: msg.src,
            body: Body {
                msg_id: Some(self.msg_id),
                in_reply_to: msg.body.msg_id,
                payload: reply_payload,
            },
        };
        info!(
            "Full response: {}",
            serde_json::to_string(&reply).expect("Couldn't serialize response")
        );
        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    gossip_glomers::run::<_, KafkaNode, _, _, _>(())
}
