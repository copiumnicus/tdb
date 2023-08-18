use crate::{
    fs::{read_or_create_default, write},
    series_block::{SeriesBlock, SeriesBlockErr},
};
use anyhow::{anyhow, Result};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct SeriesParent {
    /// when writing you always just look at the last block so thats simple stuff
    /// when reading you can rayon the blocks
    blocks: Vec<SeriesBlock>,
    max_points_per_block: u16,
}

impl Default for SeriesParent {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            max_points_per_block: 32,
        }
    }
}

lazy_static! {
    static ref PARENT_NAME: String = "PARENT".into();
}

impl SeriesParent {
    pub(crate) fn init(buf: &PathBuf) -> Result<Self> {
        // make sure exists
        let res = read_or_create_default(buf, PARENT_NAME.as_str())?;
        Ok(res)
    }
    fn write_self(&self, buf: &PathBuf) -> Result<()> {
        write(buf, PARENT_NAME.as_str(), self)
    }
    pub(crate) fn write<T>(&mut self, buf: &PathBuf, point: T) -> Result<()>
    where
        T: Clone + Serialize + for<'a> Deserialize<'a>,
    {
        // first write
        if self.blocks.len() == 0 {
            let mut block = SeriesBlock::default();
            // don't handle `at capacity` err
            block
                .write(buf, &point, self.max_points_per_block)
                .map_err(|e| anyhow!("{:?}", e))?;
            self.blocks.push(block);
        } else {
            // nth write
            match self
                .blocks
                .last_mut()
                .unwrap()
                .write(buf, &point, self.max_points_per_block)
            {
                Err(e) => match e {
                    SeriesBlockErr::BlockAtCapacity(_) => {
                        // populate next block
                        self.blocks.push(self.blocks.last().unwrap().next_block());
                        // write to next block
                        self.blocks
                            .last_mut()
                            .unwrap()
                            .write(buf, &point, self.max_points_per_block)
                            // don't handle the capacity again
                            .map_err(|e| anyhow!("{:?}", e))?
                    }
                    SeriesBlockErr::Err(e) => return Err(e),
                },
                _ => (),
            }
        }

        // update the parent declaration
        self.write_self(buf)?;

        Ok(())
    }

    pub(crate) fn read_all<T>(&self, buf: &PathBuf) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        let result: Result<Vec<Vec<T>>, _> = self
            .blocks
            .iter()
            .map(|block| block.read_all::<T>(buf))
            .collect();
        Ok(result?.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_series_parent() -> Result<()> {
        let buf = PathBuf::from("./test_parent");
        std::fs::create_dir(buf.clone())?;

        let mut parent = SeriesParent::init(&buf)?;
        for i in 0..40 {
            parent.write(&buf, i)?;
        }

        assert_eq!(parent.blocks.len(), 2);

        let res0 = parent.read_all::<i32>(&buf)?;
        // get parent fresh
        let parent = SeriesParent::init(&buf)?;
        let res1 = parent.read_all::<i32>(&buf)?;
        // check consistency
        for res in vec![res0, res1] {
            assert_eq!(res.len(), 40);
            assert_eq!(res.first().unwrap(), &0);
            assert_eq!(res.last().unwrap(), &39);
        }

        std::fs::remove_dir_all(buf)?;
        Ok(())
    }
}
