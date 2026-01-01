use std::{
    fs::{self, File, OpenOptions}, io::{Seek, SeekFrom}, path::{Path, PathBuf}
};

use anyhow::{Context, Result};
use bincode::{config, encode_into_std_write};

use crate::engine::Entry;

pub struct WorkingFile {
    file: File,
    path: PathBuf,
    size_b: usize,
}

impl WorkingFile {
    pub fn open(directory: &Path, id: usize) -> Result<Self> {
        // Working file is opened once and when closed, it's considered IMMUTABLE file
        let file_path = directory.join(format!("working_file_{id}"));
        let file = Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&file_path)
                .context("Couldn't create Working file")?,
            path: file_path,
            size_b: 0,
        };
        Ok(file)
    }

    pub fn append(&mut self, entry: &Entry) -> Result<usize> {
        self.file.seek(SeekFrom::End(0))?;
        let bytes_written = encode_into_std_write(entry, &mut self.file, config::standard())?;
        self.size_b += bytes_written;
        Ok(bytes_written)
    }

    pub fn bytes_count(&self) -> usize {
        self.size_b
    }

    pub fn get_working_file_id(directory: &Path) -> Result<usize> {
        Ok(
            fs::read_dir(directory)? // TODO: better handle error. create directory if missing?
                .filter(|entry| {
                    let ent = entry.as_ref().expect("Directory Entry can not be opened");
                    ent.file_name()
                        .to_str()
                        .unwrap_or_default()
                        .contains("working_file")
                })
                .count(),
        )
    }

    pub fn get_file_name(&self) -> String {
        return self
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
    }

    pub fn get_mut_file_ref(&mut self) -> &mut File {
        return &mut self.file;
    }
}
