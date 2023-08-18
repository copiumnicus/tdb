use crate::fs::{mby_create_dir, template_and_mby_create};
use anyhow::Result;
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
