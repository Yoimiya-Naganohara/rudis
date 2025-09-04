use std::io::Error;

use tokio::io::{self, AsyncBufReadExt, AsyncReadExt};

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}
pub struct RespParser {
    buffer: Vec<u8>,
}
impl RespParser {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }
    pub async fn read_value<R: AsyncBufReadExt + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> tokio::io::Result<RespValue> {
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line.is_empty() {
            return Err(Error::new(io::ErrorKind::UnexpectedEof, "Empty line"));
        }
        let line = line.trim_end_matches("\r\n");
        if line.is_empty() {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty line after trim",
            ));
        }
        match line.as_bytes()[0] as char {
            '+' => Ok(RespValue::SimpleString(line[1..].to_string())),
            '-' => Ok(RespValue::Error(line[1..].to_string())),
            ':' => Ok(RespValue::Integer(line[1..].parse::<i64>().map_err(
                |_| Error::new(io::ErrorKind::InvalidData, "Invalid Integer"),
            )?)),
            '$' => self.read_bulk_string(reader, &line[1..]).await,
            '*' => self.read_array(reader, &line[1..]).await,
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid RESP type",
            )),
        }
    }

    pub async fn read_bulk_string<R: AsyncBufReadExt + Unpin>(
        &mut self,
        reader: &mut R,
        line: &str,
    ) -> io::Result<RespValue> {
        let length: i64 = line
            .parse()
            .map_err(|_| (Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length")))?;
        if length == -1 {
            return Ok(RespValue::BulkString(None));
        }
        if length < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Nagative bulk string length",
            ));
        }
        let mut buffer = vec![0u8; length as usize + 2];
        let _ = reader.read_exact(&mut buffer).await?;
        if buffer[length as usize] != b'\r' || buffer[length as usize + 1] != b'\n' {
            return Err(io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid bulk string terminator",
            ));
        }
        let data = String::from_utf8(buffer[..length as usize].to_vec()).map_err(|_| {
            io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid UTF-8 in bulk string",
            )
        })?;
        Ok(RespValue::BulkString(Some(data)))
    }

    pub async fn read_array<R: AsyncBufReadExt + Unpin>(
        &mut self,
        reader: &mut R,
        line: &str,
    ) -> io::Result<RespValue> {
        let count: usize = line
            .parse()
            .map_err(|_| io::Error::new(std::io::ErrorKind::InvalidData, "Invalid array length"))?;
        let mut elements = Vec::with_capacity(count);
        for _ in 0..count {
            elements.push(Box::pin(self.read_value(reader)).await?);
        }
        Ok(RespValue::Array(elements))
    }
}
