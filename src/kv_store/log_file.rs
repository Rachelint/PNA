use log::info;
use serde_derive::{Deserialize, Serialize};
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
    InvalidPath {
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} encode log {:?} failed: {}", location, log, source))]
    EncodeLog {
        source: serde_json::Error,
        location: Location,
        log: LogItem,
    },

    #[snafu(display("{} decode log {:?} failed: {}", location, json_str, source))]
    DecodeLog {
        source: serde_json::Error,
        location: Location,
        json_str: String,
    },

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

    #[snafu(display("{} unknown log {:?}", location, item))]
    UnknownCmd { location: Location, item: LogItem },
}

type Result<T, E = Error> = std::result::Result<T, E>;

// log file //////////////////////////////////////////////////
pub struct LogFile {
    cache: HashMap<String, String>,
    file: File,
    // path: PathBuf,
    // mutable: bool,
}

#[allow(unused)]
impl LogFile {
    pub fn new(path: &Path) -> Result<LogFile> {
        // process before to assert path exist
        if !path.exists() {
            return Err(Error::InvalidPath { location: location!(), path: path.into() });
        }

        // init cache
        let cache = load_from_disk(path)?;

        // open file
        info!("open log_file:{} for writing", path.display());
        let file = File::options()
            .append(true)
            .open(path)
            .context(OpenLogFileSnafu { path })?;

        Ok(LogFile { cache, file })
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
        write_disk(&mut self.file, item.clone())?;
        let _ = self.cache.remove(&item.key);

        Ok(())
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
        let item = LogEncoder::decode(&json_str).context(DecodeLogSnafu { json_str })?;
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
    let json_str = LogEncoder::encode(&log).context(EncodeLogSnafu { log })? + "\n";
    fout.write_all(json_str.as_bytes())
        .context(WriteFileSnafu { json_str })?;

    Ok(())
}

// log //////////////////////////////////////////////////
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct LogItem {
    cmd: String,
    key: String,
    #[serde(default)]
    value: Option<String>,
}

#[allow(unused)]
impl LogItem {
    pub fn new(cmd: String, key: String, value: Option<String>) -> LogItem {
        LogItem { cmd, key, value }
    }
}

#[allow(unused)]
pub struct LogEncoder;

#[allow(unused)]
impl LogEncoder {
    pub fn encode(log: &LogItem) -> serde_json::Result<String> {
        serde_json::to_string(log)
    }

    pub fn decode(json_str: &str) -> serde_json::Result<LogItem> {
        serde_json::from_str(json_str)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    use crate::kv_store::log_file::LogEncoder;

    // use assert_cmd::assert;
    use super::{write_disk, LogFile, LogItem};

    #[test]
    fn crud() {
        let test_file = tempfile::NamedTempFile::new().unwrap();
        let mut test_log_file = LogFile::new(test_file.path()).unwrap();

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
        let test_log_file = LogFile::new(test_file.path()).unwrap();
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
    fn test_log_item_serde() {
        // decode valid
        let test_json1 = r#"
        {
            "cmd": "get",
            "key": "key1",
            "value": null
        }
        "#;
        let test_log1 = LogEncoder::decode(test_json1).unwrap();
        assert_eq!(test_log1.cmd, "get");
        assert_eq!(test_log1.key, "key1");
        assert!(test_log1.value.is_none());

        // decode invalid
        let test_json2 = r#"
        {
            "cmd": "get",
            "inv_key": "key1",
            "inv_value": null,
        }
        "#;
        let test_log2 = LogEncoder::decode(test_json2);
        assert!(test_log2.is_err());

        // encode
        let test_log3 = LogItem::new(
            "set".to_owned(),
            "key2".to_owned(),
            Some("value2".to_owned()),
        );

        let test_log4 = LogItem::new("get".to_owned(), "key3".to_owned(), None);
        let res3 = LogEncoder::encode(&test_log3);
        let res4 = LogEncoder::encode(&test_log4);
        assert!(res3.is_ok());
        assert!(res4.is_ok());
        let test_json3 = res3.unwrap();
        let test_json4 = res4.unwrap();
        assert!(test_json3.contains("set"));
        assert!(test_json3.contains("key2"));
        assert!(test_json3.contains("value2"));
        assert!(test_json4.contains("get"));
        assert!(test_json4.contains("key3"));
        assert!(test_json4.contains("null"));
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
