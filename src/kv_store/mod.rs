mod log_file;

use log::{warn, info, error};
use snafu::{Location, Snafu, location};
use std::{collections::HashMap, path::{PathBuf, Path}};
use walkdir::WalkDir;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} Init KvStore path:{} not found", location, path.display()))]
    InitStore {
        location: Location,
        path: PathBuf,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KvStore {
    mem_store: HashMap<String, String>,
}
impl KvStore {
    // open
    pub fn open(path: impl Into<PathBuf>) -> Result<()> {
        let path: PathBuf = path.into();
        // check path's valid
        // walk around the dir and get files
        let res = get_file_paths(path.as_path());
        if res.is_none() {
            error!("open kv_store from {} failed", path.display());
            return Err(Error::InitStore { location: location!(), path });
        }

        // iter the files, build the hash_index for each file
        
        // one mutable and multi immutable, only write to mutable
        Ok(())
    }

    pub fn new() -> Self {
        KvStore {
            mem_store: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        // query the mem store
        self.mem_store.get(&key).cloned()
    }

    pub fn set(&mut self, key: String, value: String) {
        // first insert to file

        // update in the mem
        let _ = self.mem_store.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        // first remove it in the mem

        // remove it in the disk
        let _ = self.mem_store.remove(&key);
    }
}

// get file paths and partition them
fn get_file_paths(path: impl AsRef<Path>) -> Option<(String, Vec<String>)> {
    let path = path.as_ref();
    if !path.exists() {
        return None;
    }

    let mut mut_file_path = String::default();
    let mut imut_file_paths = Vec::new();
    for entry in WalkDir::new(path)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        let f_name = String::from(entry.file_name().to_string_lossy());
        let f_path = String::from(entry.path().to_string_lossy());
        
        
        // todo should use reg to decide whether the file is valid
        if f_name.starts_with("mutable") {
            mut_file_path = f_path;
        } else if f_name.starts_with("immutable") {
            imut_file_paths.push(f_path);
        } else {
            warn!("invalid file:{} in data dir", f_path);
        }
    }
    info!("mut_file_path:{:?}, imut_file_paths:{:?}", mut_file_path, imut_file_paths);

    Some((mut_file_path, imut_file_paths))
}


#[cfg(test)]
mod tests {
    // use assert_cmd::assert;
    use tempfile::TempDir;
    // use crate::KvStore;
    use super::get_file_paths;

    #[test]
    fn test_open() {
        // invalid file in dir, empty ret
        assert!(get_file_paths("rrrrrrrrrrr").is_none());

        // valid
        let temp_dir = TempDir::new().unwrap();
        // empty dir, empty ret
        let res = get_file_paths(temp_dir.path());
        assert!(res.is_some());
        let res = res.unwrap();
        assert!(res.0.is_empty());
        assert!(res.1.is_empty());

        // valid
        let _mut = tempfile::Builder::new()
            .prefix("mutable")
            .suffix(".json")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        let _imut1 = tempfile::Builder::new()
            .prefix("immutable1")
            .suffix(".json")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        let _imut2 = tempfile::Builder::new()
            .prefix("immutable2")
            .suffix(".json")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();
        // let _ = KvStore::open("./test_dir");
        let res = get_file_paths(temp_dir.path());
        assert!(res.is_some());
        let res = res.unwrap();
        assert!(format!("{:?}", res.0).contains("mutable.json"));
        assert!(format!("{:?}", res.1).contains("immutable1.json"));
        assert!(format!("{:?}", res.1).contains("immutable2.json"));
    }
}