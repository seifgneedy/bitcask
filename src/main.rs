use std::path::Path;

use bitcask::{BitcaskHandler, Options};


pub fn main() {
    let directory_name = Path::new("/Users/seifgneedy/Coding/Rust/bitcask/test");
    let mut options = Options::default();
    options.read_write = true;
    let mut handler = BitcaskHandler::open(directory_name, Some(options)).unwrap();
    handler.put("key".as_bytes(), "value".as_bytes()).expect("Error Putting K-V in bitcask");
    handler.put("key2".as_bytes(), "value2".as_bytes()).expect("Error Putting K-V in bitcask");
    handler.put("key4".as_bytes(), "value4".as_bytes()).expect("Error Putting K-V in bitcask");
    let val = handler.get(b"key4").unwrap();
    let str_val = String::from_utf8(val).unwrap();
    println!("Value is {}", str_val);
}
