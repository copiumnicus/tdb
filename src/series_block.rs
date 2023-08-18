use crate::fs::{read, read_or_default, write};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct SeriesBlock {
    /// at the same time the file name
    pub id: u64,
    /// the number of points in the block atm
    pub point_count: u16,
    /// from timestamp
    pub from: Duration,
    /// to timestamp
    pub to: Duration,
}

#[derive(Debug)]
pub enum SeriesBlockErr {
    BlockAtCapacity(usize),
    Err(anyhow::Error),
}

impl SeriesBlockErr {
    pub fn map(e: anyhow::Error) -> Self {
        Self::Err(e)
    }
}

impl SeriesBlock {
    pub(crate) fn next_block(&self) -> Self {
        let mut next = Self::default();
        next.id = self.id + 1;
        next
    }
    /// updates self in place, the declaration that is
    pub(crate) fn write<T>(
        &mut self,
        buf: &PathBuf,
        point: T,
        max_points: u16,
    ) -> Result<(), SeriesBlockErr>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        let file = self.id.to_string();
        let file = file.as_str();
        // read the block first
        let mut block: Vec<T> = read_or_default(buf, file).map_err(SeriesBlockErr::map)?;
        if (block.len() as u16) >= max_points {
            return Err(SeriesBlockErr::BlockAtCapacity(block.len()));
        }
        // update block
        block.push(point);
        // write the block
        write(buf, file, &block).map_err(SeriesBlockErr::map)?;

        // update self declaration
        self.point_count = block.len() as u16;
        self.to = timed::now();
        // if first write also update from
        if block.len() == 1 {
            self.from = timed::now();
        }
        Ok(())
    }

    pub(crate) fn read<T>(&self, buf: &PathBuf) -> Result<Vec<T>>
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        let file = self.id.to_string();
        read_or_default(buf, file.as_str())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_write() -> Result<()> {
        let buf = PathBuf::from("./");
        let mut block = SeriesBlock::default();
        block.write(&buf, 42usize, 2).unwrap();
        block.write(&buf, 420usize, 2).unwrap();

        let now = timed::now();

        assert_eq!(block.id, block.id);
        assert!(block.from < now);
        assert!(block.to <= now);
        assert!(block.from < block.to);

        assert!(block.write(&buf, 234usize, 2).is_err());

        std::fs::remove_file(buf.join("0"))?;

        Ok(())
    }
}
