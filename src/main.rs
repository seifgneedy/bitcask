use std::path::Path;
use anyhow::{Context, Result};

use bitcask::{BitcaskHandler, Options};


pub fn main() -> Result<()> {
    let directory_name = Path::new("test/");
    let mut options = Options::default();
    options.read_write = true;
    options.max_data_size = 100;
    let mut handler = BitcaskHandler::open(directory_name, Some(options)).unwrap();

    // wf_0
    handler.put("key".as_bytes(), "value32423423423432432423423".as_bytes()).context("Error Putting K-V in bitcask")?;
    handler.put("key2".as_bytes(), "value334253245324324234234234234234324324324lkjsdf3242342sdfsdfsdf3".as_bytes()).context("Error Putting K-V in bitcask")?;
    // wf_1
    handler.put("key3".as_bytes(), "value33425324532432423423423423423432432432432423423".as_bytes()).context("Error Putting K-V in bitcask")?;
    handler.put("key3.4".as_bytes(), "value3.4".as_bytes()).context("Error Putting K-V in bitcask")?;
    // wf_2
    handler.put("key4".as_bytes(), "value4".as_bytes()).context("Error Putting K-V in bitcask")?;
    let val = handler.get(b"key4").unwrap(); // This should get from current working file
    let str_val = String::from_utf8(val).unwrap();
    println!("Value4 is {}", str_val);
    let val = handler.get(b"key3").unwrap(); // This should get from middle working file
    let str_val = String::from_utf8(val).unwrap();
    println!("Value3 is {}", str_val);
    let val = handler.get(b"key").unwrap(); // This should get from first working file
    let str_val = String::from_utf8(val).unwrap();
    println!("Value1 is {}", str_val);
    // Delete key2
    handler.delete(b"key2")?;
    let _val = handler.get(b"key2")?;

    Ok(())
}
