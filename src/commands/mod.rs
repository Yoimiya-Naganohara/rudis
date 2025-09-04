// Commands module for Rudis
// Handles parsing and executing Redis commands

use crate::{
    database::SharedDatabase,
    networking::resp::RespValue,
};

#[derive(Debug)]
pub enum Command {
    // TODO: Add command variants
    Ping(Option<String>),
    Get(String),
    Set(String, String),
    Del(String)
}

impl Command {
    pub fn parse(resp_value: &RespValue) -> Option<Self> {
        match resp_value {
            RespValue::Array(elements) if !elements.is_empty() => {
                let command_name = match &elements[0] {
                    RespValue::BulkString(Some(name)) => name.to_uppercase(),
                    _ => return None,
                };
                match command_name.as_str() {
                    "PING" => {
                        match elements.len() {
                            1 => Some(Command::Ping(None)),
                            2 => {
                                if let RespValue::BulkString(Some(msg)) = &elements[1] {
                                    Some(Command::Ping(Some(msg.clone())))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    }
                    "GET" => {
                        if elements.len() == 2 {
                            if let RespValue::BulkString(Some(key)) = &elements[1] {
                                Some(Command::Get(key.clone()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    "SET" => {
                        if elements.len() == 3 {
                            if let (
                                RespValue::BulkString(Some(key)),
                                RespValue::BulkString(Some(value)),
                            ) = (&elements[1], &elements[2])
                            {
                                Some(Command::Set(key.clone(), value.clone()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub async fn execute(&self, db: &SharedDatabase) -> String {
        let mut db_guard = db.lock().await;
        match self {
            Self::Ping(None) => "+PONG\r\n".to_string(),
            Self::Ping(Some(msg)) => format!("+{}\r\n", msg),
            Self::Get(key) => match db_guard.get(key) {
                Some(value) => {
                    format!("${}\r\n{}\r\n", value.len(), value)
                }
                None => "$-1\r\n".to_string(),
            },
            Self::Set(key, value) => {
                db_guard.set(key.clone(), value.clone());
                "+OK\r\n".to_string()
            },
            Self::Del(key)=>{
                "+OK\r\n".to_string()
            }
        }
    }
}
