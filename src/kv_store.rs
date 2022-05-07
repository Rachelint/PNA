use log::{debug, error, info};
use snafu::{Location, ResultExt, Snafu};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use walkdir::WalkDir;

use crate::log_file::{LogFile, LogFileBuilder};
use crate::{
    compactor::{CompactorBuilder, CompactorMode},
    log_file::Error as LogFileError,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} Init store path {} not found", location, path.display()))]
    Open {
        source: LogFileError,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} set {} {} in store: {}", location, key, value, source))]
    Set {
        source: LogFileError,
        location: Location,
        key: String,
        value: String,
    },

    #[snafu(display("{} get {} in store: {}", location, key, source))]
    Get {
        source: LogFileError,
        location: Location,
        key: String,
    },

    #[snafu(display("{} rm {} in store: {}", location, key, source))]
    Rm {
        source: LogFileError,
        location: Location,
        key: String,
    },

    #[snafu(display("{} compact mut_file {} failed: {}", location, path.display(), source))]
    Compact {
        source: crate::compactor::Error,
        location: Location,
        path: PathBuf,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct KvStore {
    log_files: Arc<RwLock<LogFiles>>,
}

pub struct LogFiles {
    pub mutable: Box<RwLock<dyn LogFile>>,
    pub immutables: Vec<Box<RwLock<dyn LogFile>>>,
    pub next_id: usize,
    pub dir_path: PathBuf,
}

impl LogFiles {
    pub fn new(
        mutable: Box<RwLock<dyn LogFile>>,
        immutables: Vec<Box<RwLock<dyn LogFile>>>,
        next_id: usize,
        dir_path: PathBuf,
    ) -> Self {
        LogFiles {
            mutable,
            immutables,
            next_id,
            dir_path,
        }
    }

    pub fn next_mut_path(&mut self) -> PathBuf {
        let mut next_mut_path = self.dir_path.to_owned();
        next_mut_path.push(format!("data_{}", self.next_id));
        self.next_id += 1;

        next_mut_path
    }
}

impl KvStore {
    // open
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        info!("kv_store open from path:{}", path.display());

        // the last is mutable, and others are immutable
        let mut id_path_pairs = get_file_paths(path.as_path()).unwrap();

        // create mut and imuts
        let create_log_file = |file_path: &Path| {
            LogFileBuilder::build(file_path, "ptr").context(OpenSnafu { path: file_path })
        };

        // if empty, create
        let mut next_id = 1;
        if id_path_pairs.is_empty() {
            info!("kv_store open from nothing");
            let mut new_mut_path = path.clone();
            new_mut_path.push("data_0");
            let _ = File::create(new_mut_path.as_path());

            Ok(KvStore {
                log_files: Arc::new(RwLock::new(LogFiles::new(
                    create_log_file(new_mut_path.as_path())?,
                    Vec::new(),
                    next_id,
                    path,
                ))),
            })
        } else {
            info!("kv_store open from files:{:?}", id_path_pairs);
            let last_pair = id_path_pairs.pop().unwrap();
            next_id = last_pair.0 + 1;
            // gen mutable
            let mut_path: PathBuf = last_pair.1.into();
            let mutable = create_log_file(mut_path.as_path())?;

            // gen immutables
            let mut immutables = Vec::with_capacity(id_path_pairs.len());
            for pair in id_path_pairs {
                let imut_path: PathBuf = pair.1.into();
                immutables.push(create_log_file(imut_path.as_path())?)
            }

            Ok(KvStore {
                log_files: Arc::new(RwLock::new(LogFiles::new(
                    mutable, immutables, next_id, path,
                ))),
            })
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        debug!("kv_store get, key:{}", key);
        let log_files_inner = self.log_files.read().unwrap();

        // check contain as this order: mut, imut.rev
        let find_target = || {
            if contains_key(&log_files_inner.mutable, &key) {
                debug!("get {} in kv_store, found in mutable", key);
                Some(&log_files_inner.mutable)
            } else {
                for immut in log_files_inner.immutables.iter() {
                    if contains_key(immut, &key) {
                        debug!("get {} in kv_store, found in immutable", key);
                        return Some(immut);
                    }
                }

                debug!("get {} in kv_store, not found", key);
                None
            }
        };

        match find_target() {
            Some(t) => {
                let mut inner = t.write().unwrap();
                inner
                    .get(key.clone())
                    .context(GetSnafu { key: key.clone() })
                    .map_err(|e: Error| {
                        error!("get {} in kv_store, found but encounter err, e:{}", key, e);
                        e
                    })
            }

            None => Ok(None),
        }
    }

    /// set just the mutable
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        debug!("kv_store set, key:{}, value:{}", key, value);

        // finish set basic logic
        let (mut_len, mut_path) = {
            let log_files_inner = self.log_files.read().unwrap();

            let mut inner = log_files_inner.mutable.write().unwrap();
            inner.set(key.clone(), value.clone()).context(SetSnafu {
                key: key.clone(),
                value: value.clone(),
            })?;
            (inner.len().context(SetSnafu { key, value })?, inner.path())
        };

        // check file's size, if too big, compact it
        if mut_len > 1024 * 1024 {
            let compactor = CompactorBuilder::build(self.log_files.clone(), CompactorMode::Simple);
            compactor
                .compact()
                .context(CompactSnafu { path: mut_path })?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        debug!("kv_store rm, key:{}", key);
        let log_files_inner = self.log_files.read().unwrap();

        let mut inner = log_files_inner.mutable.write().unwrap();
        inner.remove(key.clone()).context(RmSnafu { key })
    }
}

fn contains_key(log_file: &RwLock<dyn LogFile>, key: &str) -> bool {
    let inner = log_file.read().unwrap();
    inner.contains_key(key)
}

/// file has a id, (e.g. data_1,data_2,...,data_n => 1,2,...,n)
/// if not meet to the format, will panic! straightly
fn check_and_get_file_id(f_name: String) -> usize {
    let segs: Vec<_> = f_name.split('_').collect();
    assert_eq!(segs.len(), 2);
    assert_eq!(segs[0], "data");
    segs[1].parse::<usize>().unwrap()
}

// get file paths and partition them
fn get_file_paths(path: impl AsRef<Path>) -> Option<Vec<(usize, String)>> {
    let path = path.as_ref();
    if !path.exists() {
        return None;
    }

    let mut id_path_pairs = Vec::new();
    for entry in WalkDir::new(path)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| !e.file_type().is_dir())
    {
        let id = check_and_get_file_id(String::from(entry.file_name().to_string_lossy()));
        let f_path = String::from(entry.path().to_string_lossy());
        // @todo check and get id
        id_path_pairs.push((id, f_path))
    }

    // should sort now, beacuse the last will be used as mutable
    // @todo should ensure 1,2,3,4...n
    id_path_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    info!("get paths id_path_pairs:{:?}", id_path_pairs);

    Some(id_path_pairs)
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
        assert!(res.is_empty());

        // valid
        let _mut = tempfile::Builder::new()
            .prefix("data_0")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        let _imut1 = tempfile::Builder::new()
            .prefix("data_1")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        let _imut2 = tempfile::Builder::new()
            .prefix("data_2")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();
        // let _ = KvStore::open("./test_dir");
        let res = get_file_paths(temp_dir.path());
        assert!(res.is_some());
        let res = res.unwrap();
        assert!(format!("{:?}", res[0]).contains("data_0"));
        assert!(format!("{:?}", res[1]).contains("data_1"));
        assert!(format!("{:?}", res[2]).contains("data_2"));
    }
}
