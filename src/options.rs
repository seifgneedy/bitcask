pub struct Options {
    pub read_write: bool,
    pub sync_on_put: bool,
    pub enable_compression: bool, // to be supported later
    pub max_data_size: usize,
}

impl Options {
    pub fn default() -> Self {
        Self {
            read_write: false,
            sync_on_put: false,
            enable_compression: false,
            max_data_size: 2 * 1024 * 1024 * 1024, // 2 GB
        }
    }
}

