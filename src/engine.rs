use std::{collections::HashMap, error::Error};
use super::BitcaskHandler;
pub struct Bitcask {
    directory: String,
    // Be aware of concurrent updates to this map
    key_dir: HashMap<Vec<u8>, RecentEntry>,
}

pub struct RecentEntry {
    file_id: String, // What will be the type?
    value_size: u64,
    value_pos: String, // What will be the type?
    timestamp: u64, // Not sure about hte type.....
}

struct Entry {
    crc_checksum: u32,
    timestamp: u64,
    key_size: usize,
    value_size: usize,
    key: Vec<u8>,
    value: Vec<u8>,
}

pub struct Options {
    read_write: bool,
    sync_on_put: bool,
    enable_compression: bool, // to be supported later
}

impl Options {
    pub fn default() -> Self {
        Self {
            read_write: false,
            sync_on_put: false,
            enable_compression: false,
        }
    }
}

impl Bitcask {
    pub fn open(directory_name: String, options: Option<Options>) -> Result<BitcaskHandler, Box<dyn Error>> {
        todo!()
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, Box<dyn Error>>{
        todo!()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        todo!()
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    pub fn list_keys(&self) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        todo!()
    }

    pub fn merge(&self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    pub fn sync(&self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    pub fn close(&self) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

