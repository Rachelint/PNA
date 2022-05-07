use log::info;
use snafu::{location, Location, ResultExt, Snafu};

use crate::kv_store::LogFiles;
use crate::log_file::{Error as LogFileError, LogFileBuilder};
use std::fs::{self, File};
use std::io::Write;
use std::mem::replace;
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{} compact process log_file {}: {}", location, path.display(), source))]
    ProcessLogFile {
        source: LogFileError,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} compact process os file {}: {}", location, path.display(), source))]
    ProcessOsFile {
        source: std::io::Error,
        location: Location,
        path: PathBuf,
    },

    #[snafu(display("{} {}", location, dscr))]
    Unknown { location: Location, dscr: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Compactor {
    fn compact(&self) -> Result<()>;
}

/// this compactor may call LogFile's scan to get all cmds,
/// then write to the new file and generate new LogFile to return
struct SimpleCompactor {
    log_files: Arc<RwLock<LogFiles>>,
}

impl SimpleCompactor {
    pub fn new(log_files: Arc<RwLock<LogFiles>>) -> SimpleCompactor {
        SimpleCompactor { log_files }
    }
}

/// just straightly return log_file
impl Compactor for SimpleCompactor {
    fn compact(&self) -> Result<()> {
        // create a new mutable, and push old mutable to immutable,
        // should finish immediately
        {
            let mut log_files_inner = self.log_files.write().unwrap();
            let new_mut_path = log_files_inner.next_mut_path();
            let _ = File::create(new_mut_path.as_path()).context(ProcessOsFileSnafu {
                path: new_mut_path.clone(),
            })?;
            info!("in compact, switch the mutable file to {}", new_mut_path.display());
            let new_mut_file = LogFileBuilder::build(&new_mut_path, "ptr")
                .context(ProcessLogFileSnafu { path: new_mut_path })?;
            let old_mut_file = replace(&mut log_files_inner.mutable, new_mut_file);

            log_files_inner.immutables.push(old_mut_file);
        }
        
        // get from the last, read lock
        let (latest_immut_path, cmds) = {
            let log_files_inner = self.log_files.read().unwrap();
            // @todo unwrap is legal?
            let latest_immut_file = log_files_inner.immutables.last().unwrap();
            let mut inner = latest_immut_file.write().unwrap();
            let latest_immut_path = inner.path();

            (
                latest_immut_path.clone(),
                inner.scan().context(ProcessLogFileSnafu {
                    path: latest_immut_path,
                })?,
            )
        };
        let cmds_print_size = if cmds.len() > 10 {
            10
        } else {
            cmds.len()
        };
        info!("in compact, latest_immut_path:{}, cmds:{:?}", latest_immut_path.display(), &cmds[0..cmds_print_size]);

        // wirte into new path, no lock
        // let mut latest_immut_compact_path = latest_immut_path;
        let mut compact_file_name = {
            if let Some(f_name) = latest_immut_path
                .file_name()
                .and_then(|f_name| f_name.to_str())
            {
                f_name.to_owned()
            } else {
                return Err(Error::Unknown {
                    location: location!(),
                    dscr: "create a new file for compacting failed".to_owned(),
                });
            }
        };
        compact_file_name.push_str(".compact");
        let mut latest_immut_compact_path = latest_immut_path;
        latest_immut_compact_path.set_file_name(compact_file_name);
        let mut latest_immut_compact_file = File::create(latest_immut_compact_path.as_path())
            .context(ProcessOsFileSnafu {
                path: latest_immut_compact_path.clone(),
            })?;
        
        for cmd in cmds {
            latest_immut_compact_file
                .write_all(cmd.as_bytes())
                .context(ProcessOsFileSnafu {
                    path: latest_immut_compact_path.clone(),
                })?;
        }

        // pop the old log_file, push the compacted_log_file
        // will remove or change the file, so should close
        // the old log file first
        {
            let mut log_files_inner = self.log_files.write().unwrap();
            let old_immut_file = log_files_inner.immutables.pop().unwrap();
            let old_immut_path = { old_immut_file.read().unwrap().path() };
            drop(old_immut_file);

            // remove and rename
            fs::remove_file(old_immut_path.as_path()).context(ProcessOsFileSnafu {
                path: old_immut_path.clone(),
            })?;
            fs::rename(
                latest_immut_compact_path.as_path(),
                old_immut_path.as_path(),
            )
            .context(ProcessOsFileSnafu {
                path: latest_immut_compact_path,
            })?;
            let new_immut_file = LogFileBuilder::build(old_immut_path.as_path(), "ptr").context(
                ProcessLogFileSnafu {
                    path: old_immut_path.clone(),
                },
            )?;
            log_files_inner.immutables.push(new_immut_file);
        }

        Ok(())
    }
}

pub struct CompactorBuilder;

impl CompactorBuilder {
    pub fn build(log_files: Arc<RwLock<LogFiles>>, mode: CompactorMode) -> Box<dyn Compactor> {
        match mode {
            CompactorMode::Simple => Box::new(SimpleCompactor::new(log_files)),
        }
    }
}

pub enum CompactorMode {
    Simple,
}

#[cfg(test)]
mod tests {
    use super::CompactorBuilder;
    use crate::{kv_store::LogFiles, log_file::LogFileBuilder};
    use std::sync::{Arc, RwLock};
    use tempfile::TempDir;

    // use assert_cmd::assert;

    #[test]
    fn compact() {
        // test log files
        // create test tmp file as log file's inner
        let temp_dir = TempDir::new().unwrap();
        let mut_file = tempfile::Builder::new()
            .prefix("data_1")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        let imut_file = tempfile::Builder::new()
            .prefix("data_0")
            .rand_bytes(0)
            .tempfile_in(temp_dir.as_ref())
            .unwrap();

        // create log_files, write some data into data_0
        let mut_log_file = LogFileBuilder::build(mut_file.path(), "ptr").unwrap();
        let old_mut_file_size = {
            let mut inner = mut_log_file.write().unwrap();
            for i in 0..500 as u32 {
                inner.set("key1".to_string(), i.to_string()).unwrap();
            }
            for i in 0..500 as u32 {
                inner.set("key2".to_string(), i.to_string()).unwrap()
            }
            inner.len().unwrap()
        };

        let immut_log_files = vec![LogFileBuilder::build(imut_file.path(), "ptr").unwrap()];
        let test_log_files = Arc::new(RwLock::new(LogFiles::new(
            mut_log_file,
            immut_log_files,
            2,
            temp_dir.path().into(),
        )));

        // compact
        let compactor =
            CompactorBuilder::build(test_log_files.clone(), super::CompactorMode::Simple);
        compactor.compact().unwrap();

        // compare the new imut's data with old_mut
        {
            let log_files_inner = test_log_files.read().unwrap();
            assert!(log_files_inner.immutables[0]
                .read()
                .unwrap()
                .path()
                .display()
                .to_string()
                .contains("data_0"));
            assert!(log_files_inner.immutables[1]
                .read()
                .unwrap()
                .path()
                .display()
                .to_string()
                .contains("data_1"));

            assert!(log_files_inner
                .mutable
                .read()
                .unwrap()
                .path()
                .display()
                .to_string()
                .contains("data_2"));
        };

        {
            // let mut new_last_imut_inner = test_log_files.read().unwrap()
            //     .immutables[1].write().unwrap();
            let log_files_inner = test_log_files.read().unwrap();
            let mut new_last_imut_inner = log_files_inner.immutables[1].write().unwrap();

            assert!(new_last_imut_inner.len().unwrap() < old_mut_file_size);
            println!(
                "origin_size:{}, cur_size:{}",
                new_last_imut_inner.len().unwrap(),
                old_mut_file_size
            );
            assert_eq!(
                new_last_imut_inner.get("key1".to_owned()).unwrap().unwrap(),
                499.to_string()
            );
            assert_eq!(
                new_last_imut_inner.get("key2".to_owned()).unwrap().unwrap(),
                499.to_string()
            );
        }

        // check current log_files' structure
    }
}
