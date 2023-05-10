use anyhow::{self, Context};

use echo::{Message, EchoNode};

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

    let mut node = EchoNode::init(&init_msg).context("Couldn't init node")?;
    node.reply(init_msg, &mut stdout)?;
    for msg in stdin_messages {
        let msg = msg.context("Couldn't deserialize message")?;
        node.reply(msg, &mut stdout)?;
    }
    Ok(())
}
