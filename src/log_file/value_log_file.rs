use super::Error as LogFileError;
use super::{log_item::LogItem, LogFile};
use crate::log_file::log_item::LogEncoder;
use log::info;
use snafu::{location, Location, OptionExt, ResultExt, Snafu};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} new log_file with invalid path {}", location, path.display()))]
    InvalidPath { location: Location, path: PathBuf },

    #[snafu(display("{} write log {} failed: {}", location, json_str, source))]
    WriteFile {
        source: std::io::Error,
        location: Location,
        json_str: String,
    },

    #[snafu(display("{} read log_file {} failed: {}", location, path.display(), source))]
    ReadFile {
        source: std::io::Error,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} open log_file {} failed: {}", location, path.display(), source))]
    OpenLogFile {
        source: std::io::Error,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} encoder failed: {}", location, source))]
    LogEncoder {
        source: super::log_item::Error,
        location: Location,
    },

    #[snafu(display("{} remove non-exist key {}", location, key))]
    RemoveNotExistKey { location: Location, key: String },

    #[snafu(display("{} unknown log {:?}", location, item))]
    UnknownCmd { location: Location, item: LogItem },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ValueLogFile {
    inner: ValueLogFileInner,
}

impl ValueLogFile {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(ValueLogFile {
            inner: ValueLogFileInner::new(path)?,
        })
    }
}

impl LogFile for ValueLogFile {
    fn set(&mut self, key: String, value: String) -> super::Result<()> {
        self.inner
            .set(key, value)
            .map_err(|e| LogFileError::LogFileSet {
                source_str: format!("{}", e),
                location: location!(),
            })
    }

    fn get(&mut self, key: String) -> super::Result<Option<String>> {
        Ok(self.inner.get(key))
    }

    fn remove(&mut self, key: String) -> super::Result<()> {
        self.inner.remove(key).map_err(|e| LogFileError::LogFileRm {
            source_str: format!("{}", e),
            location: location!(),
        })
    }

    fn scan(&mut self) -> super::Result<Vec<String>> {
        unimplemented!()
    }

    fn len(&self) -> super::Result<u64> {
        unimplemented!()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.inner.cache.contains_key(key)
    }

    fn path(&self) -> PathBuf {
        unimplemented!()
    }
}

// log file //////////////////////////////////////////////////
pub struct ValueLogFileInner {
    cache: HashMap<String, String>,
    file: File,
    // path: PathBuf,
    // mutable: bool,
}

#[allow(unused)]
impl ValueLogFileInner {
    pub fn new(path: &Path) -> Result<ValueLogFileInner> {
        // process before to assert path exist
        if !path.exists() {
            return Err(Error::InvalidPath {
                location: location!(),
                path: path.into(),
            });
        }

        // init cache
        let cache = load_from_disk(path)?;

        // open file
        info!("open log_file:{} for writing", path.display());
        let file = File::options()
            .append(true)
            .open(path)
            .context(OpenLogFileSnafu { path })?;

        Ok(ValueLogFileInner { cache, file })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let item = LogItem::new("set".to_owned(), key, Some(value));
        write_disk(&mut self.file, item.clone())?;
        let _ = self.cache.insert(item.key, item.value.unwrap());
        Ok(())
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.cache.get(&key).cloned()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let item = LogItem::new("rm".to_owned(), key, None);
        if self.cache.contains_key(&item.key) {
            write_disk(&mut self.file, item.clone())?;
            let _ = self.cache.remove(&item.key);

            Ok(())
        } else {
            Err(Error::RemoveNotExistKey {
                location: location!(),
                key: item.key,
            })
        }
    }
}

fn load_from_disk(path: impl AsRef<Path>) -> Result<HashMap<String, String>> {
    let path = path.as_ref();
    info!("init cache from file:{}", path.display());

    let fin = File::open(path).context(OpenLogFileSnafu { path })?;
    let buffered = BufReader::new(fin);
    // todo fp way to build HashMap
    let mut cache = HashMap::new();
    for line in buffered.lines() {
        let json_str = line.context(ReadFileSnafu { path })?;
        let item = LogEncoder::decode(&json_str).context(LogEncoderSnafu)?;
        match item.cmd.as_str() {
            "set" => {
                let _ = cache.insert(
                    item.key.clone(),
                    item.value.clone().context(UnknownCmdSnafu { item })?,
                );
            }
            "rm" => {
                let _ = cache.remove(&item.key);
            }
            _ => {
                return Err(Error::UnknownCmd {
                    location: location!(),
                    item: item.clone(),
                });
            }
        }
    }

    Ok(cache)
}

fn write_disk(fout: &mut File, log: LogItem) -> Result<()> {
    let json_str = LogEncoder::encode(&log).context(LogEncoderSnafu)? + "\n";
    fout.write_all(json_str.as_bytes())
        .context(WriteFileSnafu { json_str })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    // use assert_cmd::assert;
    use super::{write_disk, LogEncoder, LogItem, ValueLogFileInner};

    #[test]
    fn crud() {
        let test_file = tempfile::NamedTempFile::new().unwrap();
        let mut test_log_file = ValueLogFileInner::new(test_file.path()).unwrap();

        // set
        let kv1 = ("key1".to_owned(), "value1".to_owned());
        let kv2 = ("key2".to_owned(), "value2".to_owned());
        let kv3 = ("key3".to_owned(), "value3".to_owned());
        test_log_file.set(kv1.0.clone(), kv1.1.clone()).unwrap();
        test_log_file.set(kv2.0.clone(), kv2.1.clone()).unwrap();
        test_log_file.set(kv3.0.clone(), kv3.1.clone()).unwrap();

        // get
        let res1 = test_log_file.get(kv1.0.clone());
        let res2 = test_log_file.get(kv2.0.clone());
        let res3 = test_log_file.get(kv3.0.clone());
        assert!(res1.is_some());
        assert!(res2.is_some());
        assert!(res3.is_some());
        assert_eq!(res1.unwrap(), "value1");
        assert_eq!(res2.unwrap(), "value2");
        assert_eq!(res3.unwrap(), "value3");

        // rm
        let res3 = test_log_file.remove(kv3.0.clone());
        assert!(res3.is_ok());
        let res3 = test_log_file.get(kv3.0.clone());
        assert!(res3.is_none());

        // reopen to check replay
        drop(test_log_file);
        let test_log_file = ValueLogFileInner::new(test_file.path()).unwrap();
        let res1 = test_log_file.get(kv1.0.clone());
        let res2 = test_log_file.get(kv2.0.clone());
        let res3 = test_log_file.get(kv3.0.clone());
        assert!(res1.is_some());
        assert!(res2.is_some());
        assert!(res3.is_none());
        assert_eq!(res1.unwrap(), kv1.1);
        assert_eq!(res2.unwrap(), kv2.1);
    }

    #[test]
    fn test_write_disk() {
        // test file
        let test_file = tempfile::NamedTempFile::new().unwrap();
        let mut test_file_obj = File::create(test_file.path()).unwrap();

        // write
        let test_log1 = LogItem::new(
            "set".to_owned(),
            "key1".to_owned(),
            Some("value1".to_owned()),
        );
        let test_log2 = LogItem::new(
            "set".to_owned(),
            "key2".to_owned(),
            Some("value2".to_owned()),
        );
        let res1 = write_disk(&mut test_file_obj, test_log1.clone());
        let res2 = write_disk(&mut test_file_obj, test_log2.clone());
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        drop(test_file_obj);

        // read and compare
        let test_file_obj = File::open(test_file.path()).unwrap();
        let mut log_strs = Vec::new();
        let buffered = BufReader::new(test_file_obj);
        for line in buffered.lines() {
            // log_strs.push(line.unwrap());
            log_strs.push(LogEncoder::decode(&line.unwrap()).unwrap());
            // println!("I am here");
        }
        assert_eq!(log_strs[0], test_log1);
        assert_eq!(log_strs[1], test_log2);
    }
}
