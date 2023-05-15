use std::io::{StdoutLock, Write};

use anyhow::{self, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<Req, Res> {
    pub src: String,
    pub dest: String,
    pub body: Body<Req, Res>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<Req, Res> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload<Req, Res>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Payload<Req, Res> {
    Request(Req),
    Response(Res),
}

impl<Req, Res> Payload<Req, Res> {
    pub fn request(self) -> Option<Req> {
        match self {
            Payload::Request(request) => Some(request),
            Payload::Response(_) => None,
        }
    }
}

pub trait Node<Req, Res>
where
    Req: Serialize + DeserializeOwned,
    Res: Serialize + DeserializeOwned,
{
    fn init(&mut self, msg: Message<Req, Res>, output: &mut StdoutLock) -> anyhow::Result<()>;
    fn create_reply(&mut self, msg: Message<Req, Res>) -> anyhow::Result<Message<Req, Res>>;

    fn reply(&mut self, msg: Message<Req, Res>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let reply = self.create_reply(msg)?;
        serde_json::to_writer(&mut *output, &reply).context("Writing to stdout")?;
        output
            .write_all(b"\n")
            .context("Writing newline to stdout")?;
        Ok(())
    }
}

pub fn run<S, Req, Res>(mut node: S) -> anyhow::Result<()>
where
    S: Node<Req, Res>,
    Res: Serialize + DeserializeOwned,
    Req: Serialize + DeserializeOwned,
{
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut stdin_messages =
        serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Req, Res>>();
    let init_msg = stdin_messages.next();

    let init_msg = init_msg
        .expect("init message must be present")
        .context("could not deserialize init message")?;

    node.init(init_msg, &mut stdout)
        .context("error initialising node")?;
    for msg in stdin_messages {
        let msg = msg.context("Couldn't deserialize message")?;
        node.reply(msg, &mut stdout)?;
    }
    Ok(())
}
