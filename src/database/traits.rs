// Database operations traits for Rudis

use crate::commands::Result;
use bytes::Bytes;

// Traits
pub trait StringOp {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&self, key: &Bytes, value: Bytes);
    fn del(&self, keys: &[Bytes]) -> usize;
    fn incr(&self, key: &Bytes) -> Result<i64>;
    fn decr(&self, key: &Bytes) -> Result<i64>;
    fn incr_by(&self, key: &Bytes, value: Bytes) -> Result<i64>;
    fn decr_by(&self, key: &Bytes, value: Bytes) -> Result<i64>;
    fn append(&self, key: &Bytes, value: Bytes) -> usize;
    fn str_len(&self, key: &Bytes) -> usize;
}

pub trait HashOp {
    fn hset(&self, hash: &Bytes, field: Bytes, value: Bytes) -> Result<i64>;
    fn hget(&self, hash: &Bytes, field: &Bytes) -> Result<Option<Bytes>>;
    fn hdel(&self, hash: &Bytes, field: &Bytes) -> bool;
    fn hdel_multiple(&self, hash: &Bytes, fields: &[Bytes]) -> usize;
    fn hget_all(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hkeys(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hvals(&self, hash: &Bytes) -> Result<Vec<Bytes>>;
    fn hlen(&self, hash: &Bytes) -> Result<usize>;
    fn hexists(&self, hash: &Bytes, field: &Bytes) -> Result<bool>;
    fn hincrby(&self, hash: &Bytes, field: &Bytes, value: i64) -> Result<i64>;
    fn hincrbyfloat(&self, hash: &Bytes, field: &Bytes, value: f64) -> Result<f64>;
}

pub trait ListOp {
    fn lpush(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn rpush(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn lpop(&self, key: &Bytes) -> Option<Bytes>;
    fn rpop(&self, key: &Bytes) -> Option<Bytes>;
    fn llen(&self, key: &Bytes) -> usize;
    fn lindex(&self, key: &Bytes, index: i64) -> Option<Bytes>;
    fn lrange(&self, key: &Bytes, start: i64, end: i64) -> Result<Vec<Bytes>>;
    fn ltrim(&self, key: &Bytes, start: i64, end: i64) -> Result<()>;
    fn lset(&self, key: &Bytes, index: i64, value: Bytes) -> Result<()>;
    fn linsert(&self, key: &Bytes, ord: &str, pivot: &Bytes, value: Bytes) -> Result<i64>;
}

pub trait SetOp {
    fn sadd(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn srem(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>>;
    fn scard(&self, key: &Bytes) -> usize;
    fn sismember(&self, key: &Bytes, member: &Bytes) -> bool;
    fn sinter(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
    fn sunion(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
    fn sdiff(&self, keys: &[Bytes]) -> Result<Vec<Bytes>>;
}

pub trait SortedSetOp {
    fn zadd(&self, key: &Bytes, pair: &[(f64, Bytes)]) -> usize;
    fn zrem(&self, key: &Bytes, values: &[Bytes]) -> usize;
    fn zrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>>;
    fn zrange_by_score(&self, key: &Bytes, min: f64, max: f64) -> Result<Vec<Bytes>>;
    fn zcard(&self, key: &Bytes) -> usize;
    fn zscore(&self, key: &Bytes, member: &Bytes) -> Option<f64>;
    fn zrank(&self, key: &Bytes, member: &Bytes) -> Option<usize>;
}

pub trait KeyOp {
    fn exist(&self, keys: &[Bytes]) -> usize;
    fn expire(&self, key: &Bytes, seconds: u64) -> Result<()>;
    fn ttl(&self, key: &Bytes) -> i64;
    fn keys(&self, pattern: &Bytes) -> Result<Vec<Bytes>>;
    fn flush_all(&self) -> bool;
    fn flush_db(&self) -> bool;
    fn select(&self, db: u8);
}