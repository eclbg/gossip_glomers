use std::{
    io::{BufRead, StdoutLock, Write},
    sync::mpsc::Sender
};

use anyhow::{self, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type MessageId = usize;

pub enum Event<Req, Res, Inj> {
    Message(Message<Req, Res>),
    Injected(Inj),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<Req, Res> {
    pub src: String,
    pub dest: String,
    pub body: Body<Req, Res>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<Req, Res> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    pub payload: Payload<Req, Res>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Payload<Req, Res> {
    Request(Req),
    Response(Res),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitRequest {
    Init(Init),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitResponse {
    InitOk,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

impl<Req, Res> Payload<Req, Res> {
    pub fn request(self) -> Option<Req> {
        match self {
            Payload::Request(request) => Some(request),
            Payload::Response(_) => None,
        }
    }

    pub fn response(self) -> Option<Res> {
        match self {
            Payload::Request(_) => None,
            Payload::Response(response) => Some(response),
        }
    }
}

pub trait Node<S, Req, Res, Inj = ()>
where
    Req: Serialize + DeserializeOwned,
    Res: Serialize + DeserializeOwned,
{
    fn from_init(state: S, init: Init, tx: Sender<Event<Req, Res, Inj>>) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, msg: Event<Req, Res, Inj>, output: &mut dyn Write) -> anyhow::Result<()>;
}

pub fn run<S, N, Req, Res, Inj>(init_state: S) -> anyhow::Result<()>
where
    N: Node<S, Req, Res, Inj> + Send,
    Res: Serialize + DeserializeOwned + Send + 'static,
    Req: Serialize + DeserializeOwned + Send + 'static,
    Inj: Send + 'static,
{
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitRequest, InitResponse> = serde_json::from_str(
        &stdin
            .next()
            .expect("init message must be present")
            .context("failed to read init message from stdin")?,
    )
    .context("couldn't deserialize init message")?;

    drop(stdin);

    let Payload::Request(InitRequest::Init(init)) = init_msg.body.payload else {
        panic!("first message should be init");
    };

    let (tx, rx) = std::sync::mpsc::channel();

    let mut node: N =
        Node::from_init(init_state, init, tx.clone()).context("Couldn't initialise node")?;

    std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        let stdin = stdin.lines();
        for line in stdin {
            let line = line.expect("error reading from stdin");
            let msg: Message<Req, Res> = serde_json::from_str(&line).expect("error deserializing");
            if tx.send(Event::Message(msg)).is_err() {
                return Ok::<_, anyhow::Error>(());
            }
        }
        Ok(())
    });

    let reply = Message::<InitRequest, InitResponse> {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            msg_id: None,
            in_reply_to: init_msg.body.msg_id,
            payload: Payload::<InitRequest, InitResponse>::Response(InitResponse::InitOk),
        },
    };
    serde_json::to_writer(&mut stdout, &reply).context("error writing message stdout")?;
    stdout
        .write_all(b"\n")
        .context("error writing newline to stdout")?;

    for event in rx {
        node.step(event, &mut stdout)?
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_init_serialisation() {
        let message = Message {
            src: String::from("source"),
            dest: String::from("destination"),
            body: Body {
                msg_id: Some(0),
                in_reply_to: None,
                payload: Payload::<InitRequest, InitResponse>::Request(InitRequest::Init(Init {
                    node_id: String::from("n0"),
                    node_ids: vec![String::from("n0")],
                })),
            },
        };
        let serialized = serde_json::to_string(&message).unwrap();
        assert_eq!(
            serialized,
            String::from(
                r#"{"src":"source","dest":"destination","body":{"msg_id":0,"type":"init","node_id":"n0","node_ids":["n0"]}}"#
            )
        );
    }
}
