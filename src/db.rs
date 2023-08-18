use crate::{
    fs::{mby_create_dir, template_and_mby_create},
    series_parent::{Series, SeriesParent},
    OwnsPrimaryKey,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::path::PathBuf;

pub struct Tdb {
    pub(crate) mount_dir: PathBuf,
}

impl Tdb {}

pub fn custom(path: &PathBuf) -> Result<Tdb> {
    mby_create_dir(path)?;
    Ok(Tdb {
        mount_dir: path.clone(),
    })
}

/// use default mount $HOME/tdb
pub fn new() -> Result<Tdb> {
    let home = std::env::var("HOME")?;
    let base = PathBuf::from(&home);
    Ok(Tdb {
        mount_dir: template_and_mby_create(&base, "tdb")?,
    })
}

pub fn write_in_thread<T, K>(series_name: impl ToString, point: T)
where
    T: OwnsPrimaryKey<K> + Serialize + for<'a> Deserialize<'a> + Clone + Sync + Send + 'static,
    K: PartialEq + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Sync + Send,
{
    let series_name = series_name.to_string();
    std::thread::spawn(|| {
        if let Err(e) = write(series_name, point) {
            println!("Failed to write {:?}", e)
        }
    });
}

pub fn write<T, K>(series_name: impl ToString, point: T) -> Result<()>
where
    T: OwnsPrimaryKey<K> + Serialize + for<'a> Deserialize<'a> + Clone,
    K: PartialEq + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
{
    let db = new()?;
    let mut parent = Series::init_with_create_dir(db.mount_dir, series_name)?;
    parent.write(point)
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_create() -> Result<()> {
        let buf = PathBuf::from("./test");
        // can't do much now
        println!("make custom");
        let tdb = custom(&buf)?;
        println!("exist0");
        assert!(tdb.mount_dir.exists());

        std::fs::remove_dir(tdb.mount_dir.clone())?;

        println!("exist1");
        assert!(!tdb.mount_dir.exists());

        Ok(())
    }
}
