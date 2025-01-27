use async_trait::async_trait;
use flyio_gossip_glomers_challenge::db::Db;
use log::info;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;
use uuid::Uuid;

fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    Runtime::new().with_handler(handler).run().await
}

#[derive(Default)]
struct Handler {
    db: OnceCell<Db>,
    addressbook: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

impl Handler {
    async fn init_db(&self, node_id: &str) -> Result<()> {
        self.db
            .get_or_try_init(|| async { Db::new(node_id) })
            .await?;
        Ok(())
    }
}

fn add_known_peer(addressbook: Arc<Mutex<HashMap<String, HashSet<String>>>>, peer: &str) {
    let mut addressbook = addressbook.lock().unwrap();
    if addressbook.get(peer).is_none() {
        addressbook.insert(peer.to_string(), HashSet::new());
    }
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, rt: Runtime, req: Message) -> Result<()> {
        let msg: Result<Request> = req.body.as_obj();

        match msg {
            Ok(Request::Init { node_ids }) => {
                // init has been proceeded by the runtime
                // we now know the node_id
                if rt.node_id().is_empty() {
                    return Err("node id is empty".into());
                }

                for node_id in node_ids {
                    add_known_peer(self.addressbook.clone(), &node_id);
                }

                self.init_db(rt.node_id()).await?;
            }
            // challenge #1
            Ok(Request::Echo { .. }) => {
                let resp = req.body.clone().with_type("echo_ok");
                return rt.reply(req, resp).await;
            }

            // challenge #2 - unique id
            Ok(Request::Generate {}) => {
                let id = Uuid::new_v4();
                let mut resp = req.body.clone().with_type("generate_ok");
                resp.extra.insert("id".to_string(), id.to_string().into());
                return rt.reply(req, resp).await;
            }

            // challenge #3 - broadcast & topology
            Ok(Request::Broadcast { message }) => {
                self.db
                    .get()
                    .ok_or("node is not initialized".to_string())?
                    .set_broadcast_id(message)
                    .await?;

                let neighbours: Vec<String> = {
                    let addressbook = self.addressbook.lock().unwrap();
                    addressbook.keys().cloned().collect()
                };

                for node in neighbours {
                    if node == rt.node_id() {
                        continue;
                    }

                    rt.call_async(node, Request::Broadcast { message });
                }

                let mut resp = req.body.clone().with_type("broadcast_ok");
                resp.extra.clear();
                return rt.reply(req, resp).await;
            }

            Ok(Request::BroadcastOk {}) => info!("Broadcast Ok"),

            Ok(Request::Read {}) => {
                let values = self
                    .db
                    .get()
                    .ok_or("node is not initialized".to_string())?
                    .seen_broadcast_values()
                    .await?;

                let mut resp = req.body.clone().with_type("read_ok");
                resp.extra.insert("messages".to_string(), values.into());
                return rt.reply(req, resp).await;
            }

            Ok(Request::ReadOk { messages }) => {
                for message in messages {
                    self.db
                        .get()
                        .ok_or("node is not initialized".to_string())?
                        .set_broadcast_id(message)
                        .await?;
                }
            }

            Ok(Request::Topology { topology }) => {
                {
                    let mut addressbook = self.addressbook.lock().unwrap();
                    for (node, peers) in topology {
                        addressbook.entry(node).or_default().extend(peers);
                    }
                }

                let mut resp = req.body.clone().with_type("topology_ok");
                resp.extra.clear();
                return rt.reply(req, resp).await;
            }

            _ => {
                eprintln!("Raw Body: {:?}", req.body);
                eprintln!("Message: {:?} failed to match", msg);
            }
        };

        done(rt, req)
    }
}

type Topology = HashMap<String, Vec<String>>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Request {
    Init { node_ids: Vec<String> },
    Read {},
    ReadOk { messages: Vec<u64> },
    Generate {},
    Echo { echo: String },
    Broadcast { message: u64 },
    BroadcastOk {},
    Topology { topology: Topology },
}
