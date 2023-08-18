use crate::{fs::mby_create_file_with_default, series_parent::SeriesParent};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{path::PathBuf, time::Duration};

fn fmt_key(k: String) -> String {
    let mut hasher = Sha1::new();
    hasher.update(k.as_bytes());
    let result: Vec<u8> = hasher.finalize().to_vec();
    format!(
        "{}",
        result
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join("")
    )
}

pub struct SeriesStore {
    mount_dir: PathBuf,
    series_name_hash: String,
}

impl SeriesStore {
    pub fn new(mount_dir: PathBuf, series_name: String) -> Result<Self> {
        let series_name = fmt_key(series_name);
        mby_create_file_with_default::<SeriesParent>(&mount_dir, series_name.as_str())?;
        Ok(Self {
            mount_dir,
            series_name_hash: series_name,
        })
    }
}