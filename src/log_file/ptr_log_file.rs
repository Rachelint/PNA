use super::{Error as LogFileError, log_item};
use super::{log_item::LogItem, LogFile};
use crate::log_file::log_item::LogEncoder;
use log::{debug, info};
use snafu::{location, Location, ResultExt, Snafu};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} new log_file with invalid path {}", location, path.display()))]
    InvalidPath { location: Location, path: PathBuf },

    #[snafu(display("{} read log_file failed: {}", location, source))]
    ReadFile {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("{} open log_file {} failed: {}", location, path.display(), source))]
    OpenFile {
        source: std::io::Error,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} record in {} failed: {}", location, caller, source))]
    RecordLog {
        source: WriteDiskError,
        location: Location,
        caller: String,
    },

    #[snafu(display("{} open seek log_file failed: {}", location, source))]
    SeekFile {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("{} decode {} in {} failed: {}", location, json_str, caller, source))]
    DecodeLog {
        source: super::log_item::Error,
        location: Location,
        json_str: String,
        caller: String,
    },

    #[snafu(display("{} remove non-exist key {}", location, key))]
    RemoveNotExistKey { location: Location, key: String },

    #[snafu(display("{} remove non-exist key: {}", location, source))]
    QueryMetaData {
        source: std::io::Error,
        location: Location,
    },

    #[snafu(display("{} unknown log {:?}", location, item))]
    UnknownCmd { location: Location, item: LogItem },

    #[snafu(display("{} what the hell? {}", location, dscr))]
    Unexpected { location: Location, dscr: String },

    #[snafu(display("{} file in log_file is empty, path {}", location, path.display()))]
    EmptyFile { location: Location, path: PathBuf },
}

type Result<T, E = Error> = std::result::Result<T, E>;

// log file //////////////////////////////////////////////////
pub struct PtrLogFile {
    inner: PtrLogFileInner,
}

impl PtrLogFile {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(PtrLogFile {
            inner: PtrLogFileInner::new(path)?,
        })
    }
}

impl LogFile for PtrLogFile {
    fn set(&mut self, key: String, value: String) -> super::Result<()> {
        self.inner
            .set(key, value)
            .map_err(|e| LogFileError::LogFileSet {
                source_str: format!("{}", e),
                location: location!(),
            })
    }

    fn get(&mut self, key: String) -> super::Result<Option<String>> {
        self.inner.get(key).map_err(|e| LogFileError::LogFileGet {
            source_str: format!("{}", e),
            location: location!(),
        })
    }

    fn remove(&mut self, key: String) -> super::Result<()> {
        self.inner.remove(key).map_err(|e| LogFileError::LogFileRm {
            source_str: format!("{}", e),
            location: location!(),
        })
    }

    fn scan(&mut self) -> super::Result<Vec<String>> {
        self.inner.scan().map_err(|e| LogFileError::LogFileScan {
            source_str: format!("{}", e),
            location: location!(),
        })
    }

    fn len(&self) -> super::Result<u64> {
        self.inner.len().map_err(|e| LogFileError::LogFileLen {
            source_str: format!("{}", e),
            location: location!(),
        })
    }

    fn contains_key(&self, key: &str) -> bool {
        self.inner.index.contains_key(key)
    }

    fn path(&self) -> PathBuf {
        self.inner.path.clone()
    }
}

enum IndexEntry {
    Exist(u64),
    Removed(u64),
}

pub struct PtrLogFileInner {
    index: HashMap<String, IndexEntry>,
    file: Option<File>,
    path: PathBuf,
    // mutable: bool,
}

impl PtrLogFileInner {
    pub fn new(path: &Path) -> Result<PtrLogFileInner> {
        // process before to assert path exist
        if !path.exists() {
            return Err(Error::InvalidPath {
                location: location!(),
                path: path.into(),
            });
        }

        // init cache
        let index = build_index(path)?;

        // open file
        info!("open log_file:{} for writing", path.display());
        let file = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context(OpenFileSnafu { path })?;
        Ok(PtrLogFileInner {
            index,
            file: Some(file),
            path: path.to_path_buf(),
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        debug!("set key:{} value:{} in ptr_index_log_file", key, value);

        if self.file.is_none() {
            return Err(Error::EmptyFile {
                location: location!(),
                path: self.path.clone(),
            });
        }

        // get cursor first
        let new_cursor = self
            .file
            .as_ref()
            .unwrap()
            .stream_position()
            .context(SeekFileSnafu)?;

        // update file
        let item = LogItem::new("set".to_owned(), key, Some(value));
        write_disk(self.file.as_mut().unwrap(), item.clone()).context(RecordLogSnafu{caller: "PtrLogFile::set".to_owned()})?;

        // update index
        let _ = self.index.insert(item.key, IndexEntry::Exist(new_cursor));
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        debug!("get key:{} in ptr_index_log_file", key);

        if self.file.is_none() {
            return Err(Error::EmptyFile {
                location: location!(),
                path: self.path.clone(),
            });
        }

        // get cursor
        let cursor = if let Some(entry) = self.index.get(&key) {
            match entry {
                IndexEntry::Exist(c) => *c,
                IndexEntry::Removed(_) => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        // get log from file by cursor
        let _ = self
            .file
            .as_mut()
            .unwrap()
            .seek(std::io::SeekFrom::Start(cursor))
            .context(SeekFileSnafu)?;
        let mut buf_file = BufReader::new(self.file.as_mut().unwrap());
        let mut log_str = String::new();
        if buf_file.read_line(&mut log_str).context(ReadFileSnafu)? == 0 {
            return Err(Error::Unexpected {
                location: location!(),
                dscr: "read line and get eof".to_owned(),
            });
        }

        // decode log
        let item = LogEncoder::decode(&log_str).context(DecodeLogSnafu{ caller: "get", json_str: log_str.clone() })?;

        match item.value {
            None => Err(Error::Unexpected {
                location: location!(),
                dscr: format!("invalid log in file {}", log_str),
            }),
            Some(v) => {
                debug!(
                    "get value:{} from key:{} in ptr_index_log_file",
                    item.key, v
                );
                Ok(Some(v))
            }
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        debug!("rm key:{} in ptr_index_log_file", key);

        if self.file.is_none() {
            return Err(Error::EmptyFile {
                location: location!(),
                path: self.path.clone(),
            });
        }

        // update file
        let item = LogItem::new("rm".to_owned(), key, None);

        if (self.index.contains_key(&item.key))
            && matches!(self.index.get(&item.key).unwrap(), IndexEntry::Exist(_))
        {
            let new_cursor = self
                .file
                .as_ref()
                .unwrap()
                .stream_position()
                .context(SeekFileSnafu)?;
            write_disk(self.file.as_mut().unwrap(), item.clone()).context(RecordLogSnafu{caller: "PtrLogFile::remove".to_owned()})?;
            // update index
            let _ = self.index.insert(item.key, IndexEntry::Removed(new_cursor));
            Ok(())
        } else {
            Err(Error::RemoveNotExistKey {
                location: location!(),
                key: item.key,
            })
        }
    }

    pub fn scan(&mut self) -> Result<Vec<String>> {
        info!("scan in ptr_index_log_file");

        if self.file.is_none() {
            return Err(Error::EmptyFile {
                location: location!(),
                path: self.path.clone(),
            });
        }

        let offsets: Vec<_> = self.index.iter().map(|(_, v)| v).collect();
        let mut cmds = Vec::with_capacity(offsets.len());
        let mut fin = BufReader::new(self.file.as_mut().unwrap());

        for offset in offsets {
            let mut line = String::new();
            let offset = match offset {
                IndexEntry::Exist(o) => *o,
                IndexEntry::Removed(o) => *o,
            };
            let _ = fin.seek(SeekFrom::Start(offset)).context(SeekFileSnafu)?;
            let bytes = fin.read_line(&mut line).context(ReadFileSnafu)?;
            if bytes == 0 {
                return Err(Error::Unexpected {
                    location: location!(),
                    dscr: "scan file and get eof".to_owned(),
                });
            }

            cmds.push(line);
        }

        Ok(cmds)
    }

    pub fn len(&self) -> Result<u64> {
        if self.file.is_none() {
            return Err(Error::EmptyFile {
                location: location!(),
                path: self.path.clone(),
            });
        }

        Ok(self
            .file
            .as_ref()
            .unwrap()
            .metadata()
            .context(QueryMetaDataSnafu)?
            .len())
    }
}

fn build_index(path: impl AsRef<Path>) -> Result<HashMap<String, IndexEntry>> {
    let path = path.as_ref();
    info!("build_index from file:{}", path.display());

    let mut fin = BufReader::new(File::open(path).context(OpenFileSnafu { path })?);
    let mut index = HashMap::new();
    let mut next_cursor = fin.stream_position().context(SeekFileSnafu)?;
    loop {
        let mut line = String::new();
        let bytes = fin.read_line(&mut line).context(ReadFileSnafu)?;

        if bytes == 0 {
            info!("scan log_file:{} finish", path.display());
            break;
        }

        let item = LogEncoder::decode(&line).context(DecodeLogSnafu{ json_str: line, caller: "open"})?;
        match item.cmd.as_str() {
            "set" => {
                // todo check log valid by reg
                let _ = index.insert(item.key.clone(), IndexEntry::Exist(next_cursor));
            }
            "rm" => {
                let _ = index.insert(item.key.clone(), IndexEntry::Removed(next_cursor));
            }
            _ => {
                return Err(Error::UnknownCmd {
                    location: location!(),
                    item: item.clone(),
                });
            }
        }

        // update cursor
        next_cursor = fin.stream_position().context(SeekFileSnafu)?;
    }

    Ok(index)
}

#[derive(Debug, Snafu)]
pub enum WriteDiskError {
    #[snafu(display("{} encode {:?}: {} before write disk", location, item, source))]
    EncodeLog { source:log_item::Error, location: Location, item: LogItem },

    #[snafu(display("{} write {} to disk: {}", location, json_str, source))]
    WriteFile { source:std::io::Error, location: Location, json_str: String },
}


fn write_disk(fout: &mut File, item: LogItem) -> Result<(), WriteDiskError> {
    let json_str = LogEncoder::encode(&item).context(EncodeLogSnafu{item})? + "\n";
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
    use super::{write_disk, LogEncoder, LogItem, PtrLogFileInner};

    #[test]
    fn crud() {
        let test_file = tempfile::NamedTempFile::new().unwrap();
        let mut test_log_file = PtrLogFileInner::new(test_file.path()).unwrap();

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
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());
        let res1 = res1.unwrap();
        let res2 = res2.unwrap();
        let res3 = res3.unwrap();
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
        assert!(res3.is_ok());
        assert!(res3.unwrap().is_none());

        // reopen to check replay
        drop(test_log_file);
        let mut test_log_file = PtrLogFileInner::new(test_file.path()).unwrap();
        let res1 = test_log_file.get(kv1.0.clone());
        let res2 = test_log_file.get(kv2.0.clone());
        let res3 = test_log_file.get(kv3.0.clone());
        assert!(res1.is_ok());
        assert!(res2.is_ok());
        assert!(res3.is_ok());

        let res1 = res1.unwrap();
        let res2 = res2.unwrap();
        let res3 = res3.unwrap();
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
            log_strs.push(LogEncoder::decode(&line.unwrap()).unwrap());
        }
        assert_eq!(log_strs[0], test_log1);
        assert_eq!(log_strs[1], test_log2);
    }

    #[test]
    fn test_remove_file() {
        // drop
    }
}
