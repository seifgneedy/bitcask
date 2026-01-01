use anyhow::{Context, Result};
use bincode::{Decode, Encode, config, decode_from_std_read, encode_into_std_write};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{Options, files::WorkingFile};
use crc32fast::Hasher;

use super::BitcaskHandler;

pub struct Bitcask {
    directory: PathBuf,
    _lock: Option<File>,
    working_file: Option<WorkingFile>,
    working_file_id: Option<usize>, // Number of existing files in directory + 1
    // Be aware of concurrent updates to this map, aren't we have only one Process? BUT can have multiple threads?
    key_dir: HashMap<Vec<u8>, DirEntry>,
    options: Options,
    // IDEA: keep files opened to avoid opening for every request in a hashmap? with
    // TODO: study the feasibility of having mmap instead. That will limit our implementation on 64-bit arch?
    files_pool: HashMap<String, File>,
}

impl Bitcask {
    pub fn new(
        directory: &Path,
        lock_file: Option<File>,
        working_file: Option<WorkingFile>,
        working_file_id: Option<usize>,
        key_dir: HashMap<Vec<u8>, DirEntry>,
        options: Options,
        files_pool: HashMap<String, File>,
    ) -> Self {
        Self {
            directory: directory.to_path_buf(),
            _lock: lock_file,
            working_file,
            working_file_id,
            key_dir,
            options,
            files_pool,
        }
    }
}

#[derive(Clone, Encode, Decode)]
pub struct DirEntry {
    file_name: String,
    entry_pos: usize,
    timestamp: u64,
}

impl DirEntry {
    pub fn new(file_name: String, entry_pos: usize, timestamp: u64) -> Self {
        Self {
            file_name,
            entry_pos,
            timestamp,
        }
    }
}

#[derive(Encode, Decode)]
pub struct Entry {
    crc_checksum: u32,
    timestamp: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    is_deleted: bool,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            crc_checksum: Self::generate_checksum(timestamp, &key, &value),
            timestamp: timestamp,
            key: key,
            value: value,
            is_deleted: false,
        }
    }

    fn generate_checksum(timestamp: u64, key: &Vec<u8>, value: &Vec<u8>) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&key);
        hasher.update(&value);
        hasher.finalize()
    }

    pub fn mark_deleted(&mut self) {
        self.is_deleted = true
    }
}

impl Bitcask {
    pub fn open(directory: &Path, options: Option<Options>) -> Result<BitcaskHandler> {
        /*
         * Now we have the working file in hand and locking for only one process, What is left in this method?
         * Build the Hashmap from existing data and hint files when opening existing bitcask directory
         */
        let options = options.unwrap_or(Options::default());
        let (key_dir, files_pool) = Self::build_key_dir_map_and_files_pool(&directory)?;

        let (lock_file, working_file, working_file_id) = if options.read_write {
            let lock_file = Some(Self::try_acquire_write_lock(directory)?);
            let working_file_id = WorkingFile::get_working_file_id(directory).unwrap_or_default();
            let working_file = Some(
                WorkingFile::open(directory, working_file_id)
                    .context("Couldn't open the working file")?,
            );
            (lock_file, working_file, Some(working_file_id))
        } else {
            (None, None, None)
        };

        // TODO: if current directory has existing bitcask store, we should fill the hashmap with the values
        // in hint files maybe or loop over all working files in reverse order to build it?

        let bitcask_handler = BitcaskHandler {
            bitcask_engine: Bitcask::new(
                directory,
                lock_file,
                working_file,
                working_file_id,
                key_dir,
                options,
                files_pool,
            ),
        };

        Ok(bitcask_handler)
    }

    fn try_acquire_write_lock(directory: &Path) -> Result<File> {
        let lock_path = directory.join("bitcask.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&lock_path)
            .context("Failed to open bitcask.lock file")?;
        // PANIC if directory is already open
        lock_file
            .try_lock()
            .expect("Bitcask directory is already open for writing by another process");
        Ok(lock_file)
    }

    fn build_key_dir_map_and_files_pool(
        directory: &Path,
    ) -> Result<(HashMap<Vec<u8>, DirEntry>, HashMap<String, File>)> {
        // TODO: Handle reading from hint files(when added support) if exists, to build the map fast.
        let mut key_dir: HashMap<Vec<u8>, DirEntry> = HashMap::new();
        let mut files_pool: HashMap<String, File> = HashMap::new();
        let mut data_files_paths: Vec<PathBuf> = directory.read_dir()?.filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_name().to_str()?.contains("working_file") {
                Some(entry.path())
            } else {
                None
            }
        }).collect();

        data_files_paths.sort_by_key(|path| {
            path.file_name()
                .and_then(|s| s.to_str())
                .and_then(|s| s.strip_prefix("working_file_"))
                .and_then(|id| id.parse::<u64>().ok())
                .unwrap_or(0)
        });

        for file_path in data_files_paths {
            let file = OpenOptions::new()
                .read(true)
                .write(true) // For deletion; will be inserted in files_pool
                .open(&file_path)
                .context(format!(
                    "Error Opening data file with path{}",
                    file_path.to_str().unwrap()
                ))?;
            let mut reader = BufReader::with_capacity(64 * 1024, file); // 64 KB
            let file_name = file_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap_or_default()
                .to_string();

            loop {
                // Note: stream_position, does a system call(lseek(fd, 0, SEEK_CUR)) to get the current offset, any better way?
                let disk_entry_pos: usize = reader.stream_position()?.try_into().unwrap();
                let disk_entry: Entry =
                    match decode_from_std_read::<Entry, _, _>(&mut reader, config::standard()) {
                        Ok(e) => e,
                        Err(e) => {
                            if e.to_string().contains("UnexpectedEof") {
                                break; // reached EOF
                            } else {
                                return Err(e.into()); // real error
                            }
                        }
                    };

                if disk_entry.is_deleted {
                    if key_dir.contains_key(&disk_entry.key) {
                        key_dir.remove(&disk_entry.key);
                    }
                } else {
                // We don't need to check the timestamp as we sorted the files by id(time) already
                    key_dir.insert(
                            disk_entry.key,
                            DirEntry::new(file_name.clone(), disk_entry_pos, disk_entry.timestamp),
                        );
                }
            }

            files_pool.insert(file_name, reader.into_inner());
        }

        Ok((key_dir, files_pool))
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        let dir_entry = self
            .key_dir
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key-Value not found"))?;
        let mut data_file: &mut File = self.get_file_containing_key(dir_entry.file_name)?;
        data_file.seek(SeekFrom::Start(dir_entry.entry_pos.try_into()?))?;

        let entry: Entry = decode_from_std_read(&mut data_file, config::standard())
            .context("Error Decoding Entry from file")?;

        Ok(entry.value)
    }

    fn get_file_containing_key(&mut self, file_name: String) -> Result<&mut File> {
        if file_name == self.working_file.as_ref().unwrap().get_file_name() {
            Ok(self.working_file.as_mut().unwrap().get_mut_file_ref())
        } else {
            if self.files_pool.contains_key(&file_name) {
                Ok(self.files_pool.get_mut(&file_name).unwrap())
            } else {
                let file_path = self.directory.join(&file_name);
                let file = OpenOptions::new()
                    .read(true)
                    .write(true) // For deletion
                    .open(&file_path)
                    .context("Failed to open data file containing this Key-Value")?;
                Ok(self.files_pool.entry(file_name).or_insert(file))
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let wf = self.working_file.get_or_insert_with(|| {
            self.working_file_id = Some(0);
            WorkingFile::open(&self.directory, 0).unwrap()
        });
        let wf_bytes_count = wf.bytes_count();
        let entry = Entry::new(key.to_vec(), value.to_vec());
        let bytes_written = wf
            .append(&entry)
            .context("Error Appending to the working file")?;

        self.key_dir.insert(
            key.to_vec(),
            DirEntry::new(
                wf.get_file_name(),
                wf.bytes_count() - bytes_written,
                entry.timestamp,
            ),
        );

        // TODO: when migrating from bincode, we can have the number of bytes to be written before actually write
        // Therefore, we can move the below check before writing and refactor above insertion. To avoid having files > max size.
        let is_wf_capacity_exceeded = bytes_written + wf_bytes_count > self.options.max_data_size;
        if is_wf_capacity_exceeded {
            self.working_file_id = Some(self.working_file_id.unwrap_or_default() + 1);
            self.working_file = Some(WorkingFile::open(
                &self.directory,
                self.working_file_id.unwrap_or_default(),
            )?)
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let dir_entry = self
            .key_dir
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Key-Value not found"))?;
        let mut data_file = self.get_file_containing_key(dir_entry.file_name)?;
        data_file.seek(SeekFrom::Start(dir_entry.entry_pos.try_into()?))?;

        let mut entry: Entry = decode_from_std_read(&mut data_file, config::standard())
            .context("Error Decoding Entry from file")?;
        entry.mark_deleted();

        // rewrite it as deleted
        data_file.seek(SeekFrom::Start(dir_entry.entry_pos.try_into()?))?;
        encode_into_std_write(entry, data_file, config::standard())?;
        self.key_dir.remove(key);
        Ok(())
    }

    pub fn list_keys(&self) -> Result<Vec<Vec<u8>>> {
        Ok(self.key_dir.keys().into_iter().cloned().collect())
    }

    pub fn merge(&self) -> Result<()> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }

    pub fn close(&self) -> Result<()> {
        todo!()
    }
}
