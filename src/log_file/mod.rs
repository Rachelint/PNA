mod log_item;
mod ptr_log_file;
mod value_log_file;

use std::{
    path::{Path, PathBuf},
    sync::RwLock,
};

use snafu::{location, Location, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} set in log_file failed: {}", location, source_str))]
    LogFileSet {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} get in log_file failed: {}", location, source_str))]
    LogFileGet {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} remove in log_file failed: {}", location, source_str))]
    LogFileRm {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} scan in log_file failed: {}", location, source_str))]
    LogFileScan {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} get log_file's len failed: {}", location, source_str))]
    LogFileLen {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} build log_file failed: {}", location, source_str))]
    LogFileBuild {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} log_file remove file failed: {}", location, source_str))]
    LogFileRmFile {
        source_str: String,
        location: Location,
    },

    #[snafu(display("{} log_file rename file failed: {}", location, source_str))]
    LogFileRenameFile {
        source_str: String,
        location: Location,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;
pub trait LogFile {
    fn contains_key(&self, key: &str) -> bool;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;

    fn scan(&mut self) -> Result<Vec<String>>;

    fn len(&self) -> Result<u64>;

    fn path(&self) -> PathBuf;
}

pub struct LogFileBuilder;

impl LogFileBuilder {
    pub fn build(path: impl AsRef<Path>, mode: &str) -> Result<Box<RwLock<dyn LogFile>>> {
        match mode {
            "value" => Ok(Box::new(RwLock::new(
                value_log_file::ValueLogFile::new(path.as_ref()).map_err(|e| {
                    Error::LogFileBuild {
                        source_str: format!("{}", e),
                        location: location!(),
                    }
                })?,
            ))),
            "ptr" => Ok(Box::new(RwLock::new(
                ptr_log_file::PtrLogFile::new(path.as_ref()).map_err(|e| Error::LogFileBuild {
                    source_str: format!("{}", e),
                    location: location!(),
                })?,
            ))),

            _ => Err(Error::LogFileBuild {
                source_str: format!("err mode {}", mode),
                location: location!(),
            }),
        }
    }
}
