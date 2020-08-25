//! A module for implementing database supporting `monotree`.
use crate::*;
use hashbrown::{HashMap, HashSet};
use utils::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
#[cfg(feature = "db-rocks")]
use rocksdb::{WriteBatch, DB};
#[cfg(feature = "db-postgres")]
use postgres::{Client, NoTls};
#[cfg(feature = "db-postgres")]
use std::env;


struct MemCache {
    set: HashSet<Hash>,
    map: HashMap<Hash, Vec<u8>>,
}

impl MemCache {
    fn new() -> Self {
        MemCache {
            set: HashSet::new(),
            map: HashMap::with_capacity(1 << 12),
        }
    }

    fn clear(&mut self) {
        self.set.clear();
        self.map.clear();
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.set.contains(key) || self.map.contains_key(key)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.map.get(key) {
            Some(v) => Ok(Some(v.to_owned())),
            None => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.map.insert(slice_to_hash(key), value);
        if self.set.contains(key) {
            self.set.remove(key);
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.map.remove(key);
        self.set.insert(slice_to_hash(key));
        Ok(())
    }
}

/// A trait defining databases used for `monotree`.
pub trait Database {
    fn new(dbpath: &str) -> Self;
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn init_batch(&mut self) -> Result<()>;
    fn finish_batch(&mut self) -> Result<()>;
}

/// A database using `HashMap`.
pub struct MemoryDB {
    db: HashMap<Hash, Vec<u8>>,
}

impl Database for MemoryDB {
    fn new(_dbname: &str) -> Self {
        MemoryDB { db: HashMap::new() }
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.db.get(key) {
            Some(v) => Ok(Some(v.to_owned())),
            None => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.db.insert(slice_to_hash(key), value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.db.remove(key);
        Ok(())
    }

    fn init_batch(&mut self) -> Result<()> {
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "db-rocks")]
/// A database using rust wrapper for `RocksDB`.
pub struct RocksDB {
    db: Arc<Mutex<DB>>,
    batch: WriteBatch,
    cache: MemCache,
    batch_on: bool,
}

#[cfg(feature = "db-rocks")]
impl From<rocksdb::Error> for Errors {
    fn from(err: rocksdb::Error) -> Self {
        Errors::new(&err.to_string())
    }
}
#[cfg(feature = "db-rocks")]
impl Database for RocksDB {
    fn new(dbpath: &str) -> Self {
        let db = Arc::new(Mutex::new(
            DB::open_default(Path::new(dbpath)).expect("new(): rocksdb"),
        ));
        RocksDB {
            db,
            batch: WriteBatch::default(),
            cache: MemCache::new(),
            batch_on: false,
        }
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.cache.contains(key) {
            return self.cache.get(key);
        }
        let db = self.db.lock().expect("get(): rocksdb");
        match db.get(key)? {
            Some(value) => {
                self.cache.put(key, value.to_owned())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.cache.put(key, value.to_owned())?;
        if self.batch_on {
            Ok(self.batch.put(key, value)?)
        } else {
            let db = self.db.lock().expect("put(): rocksdb");
            Ok(db.put(key, value)?)
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.cache.delete(key)?;
        if self.batch_on {
            Ok(self.batch.delete(key)?)
        } else {
            let db = self.db.lock().expect("remove(): rocksdb");
            Ok(db.delete(key)?)
        }
    }

    fn init_batch(&mut self) -> Result<()> {
        self.batch = WriteBatch::default();
        self.cache.clear();
        self.batch_on = true;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<()> {
        self.batch_on = false;
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            let db = self.db.lock().expect("write_batch(): rocksdb");
            db.write(batch)?;
        }
        Ok(())
    }
}

#[cfg(feature = "db-sled")]
/// A database using `Sled`, a pure-rust-implmented DB.
pub struct Sled {
    db: sled::Db,
    batch: sled::Batch,
    cache: MemCache,
    batch_on: bool,
}

#[cfg(feature = "db-sled")]
impl From<sled::Error> for Errors {
    fn from(err: sled::Error) -> Self {
        Errors::new(&err.to_string())
    }
}

#[cfg(feature = "db-sled")]
impl Sled {
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

#[cfg(feature = "db-sled")]
impl Database for Sled {
    fn new(dbpath: &str) -> Self {
        let db = sled::open(dbpath).expect("new(): sledDB");
        Sled {
            db,
            batch: sled::Batch::default(),
            cache: MemCache::new(),
            batch_on: false,
        }
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.cache.contains(key) {
            return self.cache.get(key);
        }
        match self.db.get(key)? {
            Some(value) => {
                self.cache.put(key, value.to_vec())?;
                Ok(Some(value.to_vec()))
            }
            None => Ok(None),
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.cache.put(key, value.to_owned())?;
        if self.batch_on {
            self.batch.insert(key, value);
        } else {
            self.db.insert(key, value)?;
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.cache.delete(key)?;
        if self.batch_on {
            self.batch.remove(key);
        } else {
            self.db.remove(key)?;
        }
        Ok(())
    }

    fn init_batch(&mut self) -> Result<()> {
        self.batch = sled::Batch::default();
        self.cache.clear();
        self.batch_on = true;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<()> {
        self.batch_on = false;
        let batch = std::mem::take(&mut self.batch);
        self.db.apply_batch(batch)?;
        Ok(())
    }
}


/// A database using rust wrapper for `PostgreSQL`.
#[cfg(feature = "db-postgres")]
pub struct Postgres {
    db: Client,
    table_name: String,
    batch: HashMap<Vec<u8>, Vec<u8>>,
    cache: MemCache,
    batch_on: bool,
}

#[cfg(feature = "db-postgres")]
impl From<postgres::Error> for Errors {
    fn from(err: postgres::Error) -> Self {
        Errors::new(&err.to_string())
    }
}

#[cfg(feature = "db-postgres")]
impl Database for Postgres {
    fn new(dbpath: &str) -> Self {
        let mut conn = Client::connect(dbpath, NoTls).unwrap();

        // Get tables Schema and name if it is given. Default to public.smt
        let table_name = env::var("MONOTREE_TABLE_NAME").unwrap_or("smt".to_string());

        let stmt = conn.prepare(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
            key integer[],
            value integer[],
            PRIMARY KEY (key)
        );", table_name)).unwrap();

        let _ = conn.execute(&stmt, &[]).unwrap(); // panic if fail to create table

        Postgres {
            db: conn,
            table_name,
            batch: HashMap::new(),
            cache: MemCache::new(),
            batch_on: false,
        }
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.cache.contains(key) {
            return self.cache.get(key);
        }
        let stmt = self.db.prepare(&format!("SELECT value FROM {} WHERE key = $1",self.table_name))?;
        let rows: Vec<postgres::Row> = self.db.query(&stmt, &[&key])?;
        match rows.get(0) {
            None => Ok(None),
            Some(row) => {
                match row.try_get(0) {
                    Err(_) => Ok(None),
                    Ok(data) => Ok(Some(data))
                }
            }
        }
    }

    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.cache.put(key, value.to_owned())?;
        if self.batch_on {
            let key_vec: Vec<u8> = key.iter().cloned().collect();
            self.batch.insert(key_vec, value);
        } else {
            let stmt = self.db.prepare(&format!(
                "INSERT INTO {} (key, value)
                VALUES (ARRAY{:?}, ARRAY{:?})
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value;"
                ,self.table_name, key, value))?;
            self.db.execute(&stmt, &[])?;
        };
        return Ok(());
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.cache.delete(key)?;
        if self.batch_on {
            self.batch.remove(key);
        } else {
            let stmt = self.db.prepare(&format!("DELETE FROM {} WHERE key = $1;",self.table_name))?;
            self.db.execute(&stmt, &[&key])?;
        }
        return Ok(());
    }

    fn init_batch(&mut self) -> Result<()> {
        self.batch = HashMap::new();
        self.cache.clear();
        self.batch_on = true;
        Ok(())
    }

    fn finish_batch(&mut self) -> Result<()> {
        self.batch_on = false;
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            let mut stmt_str = format!("INSERT INTO {} (key, value) VALUES", self.table_name);
            for (key, value) in batch.iter() {
                stmt_str.push_str(&format!(" (ARRAY{:?},ARRAY{:?}),", key, value));
            }
            stmt_str.truncate(stmt_str.len() - 1);
            stmt_str.push_str(" ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;");

            let stmt = self.db.prepare(&stmt_str)?;
            self.db.execute(&stmt, &[])?;
        }
        Ok(())
    }
}
