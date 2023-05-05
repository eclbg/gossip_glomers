use std::io::Write;

use serde::{Deserialize, Serialize};
use anyhow::{self, Context, bail};

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Payload {
    Request(Request),
    Response(Response),
}

impl Payload {
    fn request(self) -> Option<Request> {
        match self {
            Payload::Request(request) => Some(request),
            Payload::Response(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Request {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        echo: serde_json::Value
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Response {
    InitOk,
    EchoOk {
        echo: serde_json::Value
    }
}

#[derive(Debug)]
struct Node {
    name: Option<String>,
    msg_id: usize,
}

impl Node {
    /// Creates a new [`Node`].
    fn new() -> Self {
        todo!()
    }

    fn init(msg: &Message) -> anyhow::Result<Node> {
        let payload = &msg.body.payload;
        match payload {
            Payload::Request(Request::Init { node_id, .. }) => Ok(Node { name: Some(node_id.clone()), ..Node::default() }),
            _ => bail!("Tried to initialise node with wrong payload variant"),
        }
    }

    fn create_reply(&mut self, msg: Message) -> anyhow::Result<Message> {
        let reply: Message;
        let request = msg.body.payload.request().ok_or(anyhow::anyhow!("Message payload is not a request"))?;
        match request {
            Request::Init { node_id, .. } => {
                self.name = Some(node_id.clone());
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::InitOk)
                    }
                };
            }
            Request::Echo { echo } => {
                reply = Message {
                    src: self.name.clone().unwrap(),
                    dest: msg.src.clone(),
                    body: Body {
                        msg_id: Some(self.msg_id) ,
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::Response(Response::EchoOk { echo: echo.clone() })
                    }
                };
            }
        }
        self.msg_id += 1;
        Ok(reply)
    }
}

impl Default for Node {
    fn default() -> Self {
        Self {
            name: None,
            msg_id: 0
        }
    }
}

fn main() -> anyhow::Result<()>{
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut stdin_messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    let init_msg = stdin_messages.next();
    let init_msg = match init_msg {
        Some(msg) => msg,
        None => {
            eprintln!("no message");
            panic!("No message");
        }
    };

    let init_msg = match init_msg {
        Ok(msg) => msg,
        Err(_) => {
            eprintln!("error deserializing");
            panic!("error deserializing");
        }
    };

    let mut node = Node::init(&init_msg).context("Couldn't init node")?;
    let reply = node.create_reply(init_msg)?;
    serde_json::to_writer(&mut stdout, &reply).context("Writing to stdout")?;
    stdout.write_all(b"\n").context("Writing newline to stdout")?;
    for msg in stdin_messages {
        let reply = node.create_reply(msg.context("Couldn't deserialize message")?).context("Couldn't reply to message")?;
        serde_json::to_writer(&mut stdout, &reply).context("Writing to stdout")?;
        stdout.write_all(b"\n").context("Writing newline to stdout")?;
    }
    Ok(())
}
