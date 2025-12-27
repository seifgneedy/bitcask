use std::{path::Path, vec::Vec};
use anyhow;
use crate::Options;

use super::engine::Bitcask;

pub struct BitcaskHandler {
    pub(crate) bitcask_engine: Bitcask,
}

impl BitcaskHandler {
    /// Opens new or existing Bitcask datastore with the given options.
    ///
    /// TODO: Revise with complete details.
    /// 
    /// If no options are provided, the datastore will be opened in **read-only** mode.
    ///
    /// # Arguments
    ///
    /// * `directory_name` - The path to the directory containing the Bitcask datastore.
    /// * `options` - Optional list of configuration flags controlling how the datastore is opened.
    ///
    /// # Supported Options
    /// TODO: Complete Supported Options :)
    ///
    /// * `"read_write"` — Grants this process write access to the datastore.  
    ///   **Note:** Only one process can have write access at a time.
    /// * `"sync_on_put"` — Forces a disk sync after every `put` operation for stronger durability.
    ///
    /// # Returns
    ///
    /// Returns a [`BitcaskHandler`] instance on success, or an error if the datastore
    /// cannot be opened or accessed with the requested options.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// // Open in read-only mode
    /// let handler = BitcaskHandler::open(String::from("/tmp/bitcask"), None).unwrap();
    ///
    /// // Open with write access and sync on every write
    /// let handler = BitcaskHandler::open(
    ///     &Path::new("/tmp/bitcask"),
    ///     Some(vec![String::from("read_write"), String::from("sync_on_put")])
    /// ).unwrap();
    /// ```
    pub fn open(directory: &Path, options: Option<Options>) -> Result<Self, anyhow::Error> {
        Bitcask::open(directory, options)
    }

    /// Retrieves a value by key from the Bitcask datastore.
    ///
    /// TODO: Complete with get details.
    /// 
    /// # Arguments
    ///
    /// * `key` - A byte slice representing the key to look up.
    ///
    /// # Returns
    ///
    /// Returns the value as a `Vec<u8>` if the key exists, or an error if:
    /// - The key does not exist.
    /// - The underlying file cannot be accessed.
    /// - Data corruption is detected.
    ///
    /// # Errors
    ///
    /// This function returns a [`std::io::Error`] wrapped in a [`Box<dyn Error>`]
    /// if the read operation fails.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let db = BitcaskHandler::open("/tmp/bitcask", None).unwrap();
    /// let value = db.get(b"user:1").unwrap();
    /// println!("Value: {:?}", value);
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, anyhow::Error>{
        self.bitcask_engine.get(key)
    }

    /// Stores a key-value pair in the Bitcask datastore.
    ///
    /// TODO: Complete with addition details.
    /// # Arguments
    ///
    /// * `key` - A byte slice representing the key to insert or update.
    /// * `value` - A byte slice representing the value associated with the key.
    ///
    /// # Returns
    ///
    /// Returns an empty `Vec<u8>` on success, or an error if the write operation fails.
    ///
    /// # Errors
    ///
    /// This function returns a [`std::io::Error`] wrapped in a [`Box<dyn Error>`]
    /// if the append or sync operation fails.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let db = BitcaskHandler::open("/tmp/bitcask", Some(vec!["read_write"])).unwrap();
    /// db.put(b"user:1", b"Saif").unwrap();
    /// ```
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), anyhow::Error> {
        self.bitcask_engine.put(key, value)
    }

    /// Deletes a key and its associated value from the Bitcask datastore.
    /// 
    /// TODO: Complete with deletion details.
    /// 
    /// # Arguments
    ///
    /// * `key` - A byte slice representing the key to delete.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the key was successfully deleted or did not exist.
    /// Returns an error if the operation fails due to I/O or file corruption.
    ///
    /// # Errors
    ///
    /// This function returns a [`std::io::Error`] wrapped in a [`Box<dyn Error>`]
    /// if the delete marker cannot be written to the active data file.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let db = BitcaskHandler::open("/tmp/bitcask", Some(vec!["read_write"])).unwrap();
    /// db.put(b"user:1", b"Saif").unwrap();
    /// db.delete(b"user:1").unwrap();
    /// assert!(db.get(b"user:1").is_err());
    /// ```
    pub fn delete(&self, key: &[u8]) -> Result<(), anyhow::Error> {
        self.bitcask_engine.delete(key)
    }

    /// Lists all keys currently stored in the Bitcask datastore.
    ///
    /// This method returns all keys present in the in-memory key directory.
    /// It does **not** read from disk and therefore reflects only the
    /// keys known to the active in-memory index.
    ///
    /// # Returns
    ///
    /// Returns a vector of keys as byte arrays (`Vec<Vec<u8>>`).
    /// Returns an error if the datastore is in an invalid state.
    ///
    /// # Errors
    ///
    /// This function returns a [`std::io::Error`] wrapped in a [`Box<dyn Error>`]
    /// if the key directory cannot be accessed or is corrupted.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let db = BitcaskHandler::open("/tmp/bitcask", Some(vec!["read_write"])).unwrap();
    /// db.put(b"user:1", b"Saif").unwrap();
    /// db.put(b"user:2", b"Alice").unwrap();
    ///
    /// let keys = db.list_keys().unwrap();
    /// for k in keys {
    ///     println!("Key: {:?}", String::from_utf8_lossy(&k));
    /// }
    /// ```
    pub fn list_keys(&self) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        self.bitcask_engine.list_keys()
    }

    // TODO: Fold/Reduce method to be added in later phase

    
    /// Merge multiple data files within the Bitcask datastore into a more compact form.
    ///
    /// This operation reclaims disk space by combining data files, removing deleted or outdated entries,
    /// and producing **hint files** to speed up datastore startup.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the merge completes successfully.
    /// * `Err` if an error occurs during the merge process.
    ///
    /// # Notes
    ///
    /// - This is a potentially expensive operation in terms of I/O and CPU.
    /// - It is recommended to run merge operations during low-traffic periods to avoid performance impact.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let handler = BitcaskHandler::open("data").unwrap();
    /// handler.merge().unwrap();
    /// ```
    pub fn merge(&self) -> Result<(), anyhow::Error> {
        self.bitcask_engine.merge()
    }

    /// Force any pending writes in the Bitcask datastore to be synced to disk.
    ///
    /// This ensures that all in-memory writes are persisted,
    /// reducing the risk of data loss in the event of a crash.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the sync completes successfully.
    /// * `Err` if there is an error during syncing.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let handler = BitcaskHandler::open("data", None).unwrap();
    /// handler.sync().unwrap();
    /// ```
    pub fn sync(&self) -> Result<(), anyhow::Error> {
        self.bitcask_engine.sync()
    }

    /// Close the Bitcask datastore, flushing any pending writes to disk.
    ///
    /// This should be called before shutting down the application to ensure
    /// all data is persisted and resources are freed.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the datastore closes successfully.
    /// * `Err` if there is an error during closing.
    ///
    /// # Example
    ///
    /// ```
    /// use bitcask::BitcaskHandler;
    ///
    /// let handler = BitcaskHandler::open("data", None).unwrap();
    /// handler.close().unwrap();
    /// ```
    pub fn close(&self) -> Result<(), anyhow::Error> {
        self.bitcask_engine.close()
    }
}
