use bitcask::BitcaskHandler;

pub fn main() {
    let directory_name = String::from("/tmp/bitcask");
    let handler = BitcaskHandler::open(directory_name, None).unwrap();
}
