use anyhow::Result;
use bincode::de;
use libflate::gzip::{Decoder, Encoder};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::PathBuf,
};

/// create dir if does not exist
pub(crate) fn mby_create_dir(buf: &PathBuf) -> Result<()> {
    if !buf.exists() {
        std::fs::create_dir(buf.clone())?;
    }
    Ok(())
}

/// create dir if does not exist
pub(crate) fn template_and_mby_create(buf: &PathBuf, subdir: &str) -> Result<PathBuf> {
    let mount_dir = buf.join(subdir);
    mby_create_dir(&mount_dir)?;
    Ok(mount_dir)
}

/// create file if does not exist
pub(crate) fn mby_create_file_with_default<T>(buf: &PathBuf, file: &str) -> Result<()>
where
    // if can't deser then it would be stupid to store
    T: Default + Serialize + for<'a> Deserialize<'a>,
{
    if !buf.join(file).exists() {
        // create
        write(buf, file, &T::default())?;
    }
    Ok(())
}

/// default if not exists not if fails
pub(crate) fn read_or_default<T>(buf: &PathBuf, file: &str) -> Result<T>
where
    T: Default + Serialize + for<'a> Deserialize<'a>,
{
    if !buf.join(file).exists() {
        return Ok(T::default());
    }
    read(buf, file)
}

pub(crate) fn read<T>(buf: &PathBuf, file: &str) -> Result<T>
where
    T: Default + Serialize + for<'a> Deserialize<'a>,
{
    let file = buf.join(file);
    let file = std::fs::File::open(file)?;
    let mut decoded = Vec::new();
    // read from file, decode gzip
    Decoder::new(file)?.read_to_end(&mut decoded)?;
    // deserialize bincode
    let deser: T = bincode::deserialize(&decoded)?;
    Ok(deser)
}

pub(crate) fn write<T>(buf: &PathBuf, file: &str, value: &T) -> Result<()>
where
    // if can't deser then it would be stupid to store
    T: Default + Serialize + for<'a> Deserialize<'a>,
{
    let file = buf.join(file);
    // serialize bincode
    let bin = bincode::serialize(value)?;
    // encode gzip
    let mut encoder = Encoder::new(Vec::new())?;
    encoder.write_all(bin.as_slice())?;
    let encoded = encoder.finish().into_result()?;
    // write to file
    let mut file = std::fs::File::create(file)?;
    file.write_all(encoded.as_slice())?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[derive(Default, Deserialize, Serialize, PartialEq, Eq, Debug)]
    struct TestStruct {
        a: String,
        b: usize,
        c: u64,
        d: Duration,
    }
    #[test]
    fn test_create_read_write() -> Result<()> {
        let buf = PathBuf::from("./");
        let file = "hm";
        println!("default");
        mby_create_file_with_default::<TestStruct>(&buf, file)?;

        println!("read0");
        let mut res: TestStruct = read(&buf, file)?;
        assert_eq!(res, TestStruct::default());

        res.b = 100;
        println!("write");
        write(&buf, file, &res)?;

        println!("read1");
        let res1: TestStruct = read(&buf, file)?;
        assert_eq!(res1, res);

        std::fs::remove_file(buf.join(file))?;

        Ok(())
    }
}
