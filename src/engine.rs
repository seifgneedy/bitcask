use anyhow::{Context, bail, Result};
use bincode::{Decode, Encode, config, decode_from_std_read};
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

#[derive(Encode, Decode)]
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
        }
    }

    fn generate_checksum(timestamp: u64, key: &Vec<u8>, value: &Vec<u8>) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&key);
        hasher.update(&value);
        hasher.finalize()
    }
}

impl Bitcask {
    pub fn open(
        directory: &Path,
        options: Option<Options>,
    ) -> Result<BitcaskHandler> {
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
        let data_files_paths = directory.read_dir()?.filter_map(|entry| {
            entry
                .ok()
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .unwrap_or_default()
                        .contains("working_file")
                })
                .map(|x| x.path())
        });

        for file_path in data_files_paths {
            let file = File::open(&file_path).context(format!(
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

                match key_dir.get(&disk_entry.key) {
                    Some(val) => {
                        if disk_entry.timestamp > val.timestamp {
                            key_dir.insert(
                                disk_entry.key,
                                DirEntry::new(
                                    file_name.clone(),
                                    disk_entry_pos,
                                    disk_entry.timestamp,
                                ),
                            );
                        }
                    }
                    _ => {
                        key_dir.insert(
                            disk_entry.key,
                            DirEntry::new(file_name.clone(), disk_entry_pos, disk_entry.timestamp),
                        );
                    }
                }
            }

            files_pool.insert(file_name, reader.into_inner());
        }

        Ok((key_dir, files_pool))
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> {
        let Some(dir_entry) = self.key_dir.get(key) else {
            return Err(anyhow::anyhow!("Key-Value not found"));
        };

        let mut data_file: &mut File =
            if dir_entry.file_name == self.working_file.as_ref().unwrap().get_file_name() {
                self.working_file.as_mut().unwrap().get_mut_file_ref()
            } else {
                match self.files_pool.get_mut(&dir_entry.file_name) {
                    Some(file) => file,
                    _ => {
                        let file_path = self.directory.join(&dir_entry.file_name);
                        let file = OpenOptions::new()
                            .read(true)
                            .open(&file_path)
                            .context("Failed to open data file containing this Key-Value")?;
                        self.files_pool
                            .entry(dir_entry.file_name.clone())
                            .or_insert(file)
                    }
                }
            };
        let _new_pos = data_file.seek(SeekFrom::Start(dir_entry.entry_pos.try_into()?));

        let entry: Entry = decode_from_std_read(&mut data_file, config::standard())
            .context("Error Decoding Entry from file")?;

        Ok(entry.value)
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

    pub fn delete(&self, key: &[u8]) -> Result<(), anyhow::Error> {
        let _ = key;
        todo!()
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
