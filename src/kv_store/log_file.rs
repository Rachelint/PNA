use std::{collections::HashMap, fs::File, path::{Path, PathBuf}, io::Write};
use serde_derive::{Serialize, Deserialize};
use snafu::{Snafu, Location, ResultExt};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} new log_file from {} failed: {}", location, path.display(), source))]
    NewLogFile {
        source: std::io::Error,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} new log_file from {:?} failed: {}", location, log, source))]
    EncodeLog {
        source: serde_json::Error,
        location: Location,
        log: LogItem,
    },

    #[snafu(display("{} new log_file from {} failed: {}", location, json_str, source))]
    DecodeLog {
        source: serde_json::Error,
        location: Location,
        json_str: String,
    },
    
    #[snafu(display("{} new log_file from {} failed: {}", location, json_str, source))]
    WriteFile {
        source: std::io::Error,
        location: Location,
        json_str: String,
    },
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
        // just open now, do nothing
        let file = File::options().read(true).append(true).open(path)
            .context(NewLogFileSnafu {path})?;
        
        Ok(LogFile {
            cache: HashMap::new(),
            file,
        })
    }

    pub fn set(&mut self, log: LogItem) {
        assert_eq!(log.cmd, "set");
        assert!(log.value.is_some());
        let _ = self.cache.insert(log.key, log.value.unwrap());
    }

    pub fn get(&mut self, log: LogItem) -> Option<String> {
        assert_eq!(log.cmd, "get");
        self.cache.get(&log.key).cloned()
    }

    pub fn remove(&mut self, log: LogItem) {
        assert_eq!(log.cmd, "rm");
        let _ = self.cache.remove(&log.key);
    }
}

fn write_disk(fout: &mut File, log: LogItem) -> Result<()> {
    let json_str = LogEncoder::encode(&log)
        .context(EncodeLogSnafu{log})? + "\n";
    fout.write_all(json_str.as_bytes()).context(WriteFileSnafu{json_str})?;

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

    pub fn decoder(json_str: &str) -> serde_json::Result<LogItem> {
        serde_json::from_str(json_str)
    }

}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{BufReader, BufRead, Read, SeekFrom, Seek}, thread::sleep, time::Duration};

    use crate::kv_store::log_file::LogEncoder;

    // use assert_cmd::assert;
    use super::{LogFile, LogItem, write_disk};

    #[test]
    fn crud_cache() {
        let test_file = tempfile::NamedTempFile::new().unwrap();
        let mut test_log_file = LogFile::new(test_file.path()).unwrap();
        
        // set 
        let test_set_log1 = LogItem::new("set".to_owned(), 
            "key1".to_owned(), Some("value1".to_owned()));
        let test_set_log2 = LogItem::new("set".to_owned(), 
            "key2".to_owned(), Some("value2".to_owned()));
        test_log_file.set(test_set_log1);
        test_log_file.set(test_set_log2);
        
        // get
        let test_get_log1 = LogItem::new("get".to_owned(), 
            "key1".to_owned(), None);
        let test_get_log2 = LogItem::new("get".to_owned(), 
            "key2".to_owned(), None);
        let test_get_log3 = LogItem::new("get".to_owned(), 
            "key3".to_owned(), None);
        let res1 = test_log_file.get(test_get_log1);
        let res2 = test_log_file.get(test_get_log2);
        let res3 = test_log_file.get(test_get_log3);
        assert!(res1.is_some());
        assert!(res2.is_some());
        assert!(!res3.is_some());
        assert_eq!(res1.unwrap(), "value1");
        assert_eq!(res2.unwrap(), "value2");
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
        let test_log1 = LogEncoder::decoder(test_json1).unwrap();
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
        let test_log2 = LogEncoder::decoder(test_json2);
        assert!(test_log2.is_err());

        // encode
        let test_log3 = LogItem::new("set".to_owned(), 
            "key2".to_owned(), Some("value2".to_owned()));

        let test_log4 = LogItem::new("get".to_owned(), 
            "key3".to_owned(), None);
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
        let test_log1 = LogItem::new("set".to_owned(), 
        "key1".to_owned(), Some("value1".to_owned()));
        let test_log2 = LogItem::new("set".to_owned(), 
        "key2".to_owned(), Some("value2".to_owned()));
        let res1 = write_disk(&mut test_file_obj, test_log1.clone());
        let res2 = write_disk(&mut test_file_obj, test_log2.clone());
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        drop(test_file_obj);

        // read and compare
        let test_file_obj = File::open(test_file.path()).unwrap();
        let mut log_strs = Vec::new();
        let buffered  = BufReader::new(test_file_obj);
        for line in buffered.lines(){
            // log_strs.push(line.unwrap());
            log_strs.push(LogEncoder::decoder(&line.unwrap()).unwrap());
            // println!("I am here");
        }
        assert_eq!(log_strs[0], test_log1);
        assert_eq!(log_strs[1], test_log2);
    }
}
