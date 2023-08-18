use crate::{
    fs::{read_or_create_default, write},
    series_block::{SeriesBlock, SeriesBlockErr},
};
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{hash::Hash, marker::PhantomData, path::PathBuf};

pub trait OwnsPrimaryKey<T> {
    fn get_primary_key(&self) -> T;
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SeriesParent<T, K>
where
    K: PartialEq + Eq + Hash,
{
    /// when writing you always just look at the last block so thats simple stuff
    /// when reading you can rayon the blocks
    blocks: Vec<SeriesBlock>,
    index: HashMap<K, (usize, usize)>,
    max_points_per_block: u16,
    _phantom: PhantomData<T>,
}

impl<T, K> Default for SeriesParent<T, K>
where
    K: PartialEq + Eq + Hash,
{
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            max_points_per_block: 32,
            index: HashMap::new(),
            _phantom: PhantomData::default(),
        }
    }
}

lazy_static! {
    static ref PARENT_NAME: String = "PARENT".into();
}

impl<T, K> SeriesParent<T, K>
where
    T: OwnsPrimaryKey<K> + Clone + Serialize + for<'a> Deserialize<'a>,
    K: PartialEq + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
{
    pub(crate) fn init(buf: &PathBuf) -> Result<Self> {
        // make sure exists
        let res = read_or_create_default(buf, PARENT_NAME.as_str())?;
        Ok(res)
    }
    fn write_self(&self, buf: &PathBuf) -> Result<()> {
        write(buf, PARENT_NAME.as_str(), self)
    }
    pub(crate) fn write(&mut self, buf: &PathBuf, point: T) -> Result<()> {
        // first write
        if self.blocks.len() == 0 {
            let mut block = SeriesBlock::default();
            // don't handle `at capacity` err
            self.index.insert(
                point.get_primary_key(),
                (
                    0, // block index
                    block
                        .write(buf, &point, self.max_points_per_block)
                        .map_err(|e| anyhow!("{:?}", e))?, // index within block
                ),
            );
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
                        let block_index = self.blocks.len() - 1;
                        // write to next block
                        self.index.insert(
                            point.get_primary_key(),
                            (
                                block_index, // block index
                                self.blocks
                                    .last_mut()
                                    .unwrap()
                                    .write(buf, &point, self.max_points_per_block)
                                    // don't handle the capacity again
                                    .map_err(|e| anyhow!("{:?}", e))?, // index within block
                            ),
                        );
                    }
                    SeriesBlockErr::Err(e) => return Err(e),
                },
                Ok(index_within) => {
                    self.index.insert(
                        point.get_primary_key(),
                        (self.blocks.len() - 1, index_within),
                    );
                }
            }
        }

        // update the parent declaration
        self.write_self(buf)?;

        Ok(())
    }

    pub(crate) fn read_all(&self, buf: &PathBuf) -> Result<Vec<T>> {
        let result: Result<Vec<Vec<T>>, _> = self
            .blocks
            .iter()
            .map(|block| block.read_all::<T>(buf))
            .collect();
        Ok(result?.into_iter().flatten().collect())
    }

    pub(crate) fn read_by_key(&self, buf: &PathBuf, key: &K) -> Option<T> {
        let (block, inner) = self.index.get(key)?.clone();
        let block = self.blocks.get(block)?;
        let mut blocks = block.read_all::<T>(buf).ok()?;
        if blocks.len() < inner {
            return None;
        }
        Some(blocks.remove(inner))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_series_parent() -> Result<()> {
        let buf = PathBuf::from("./test_parent");
        std::fs::create_dir(buf.clone())?;

        #[derive(Clone, Serialize, Deserialize)]
        struct TestStruct {
            id: i32,
            s: String,
        }

        impl OwnsPrimaryKey<i32> for TestStruct {
            fn get_primary_key(&self) -> i32 {
                self.id
            }
        }

        let mut parent = SeriesParent::init(&buf)?;
        for id in 0..40 {
            parent.write(
                &buf,
                TestStruct {
                    s: format!("hi {:?}", id),
                    id,
                },
            )?;
        }

        assert_eq!(parent.blocks.len(), 2);

        let res0 = parent.read_all(&buf)?;
        // get parent fresh
        let parent = SeriesParent::<TestStruct, _>::init(&buf)?;

        assert_eq!(parent.index.get(&3).unwrap().clone(), (0, 3));
        assert_eq!(parent.index.get(&35).unwrap().clone(), (1, 3));

        assert_eq!(parent.read_by_key(&buf, &3).unwrap().s.as_str(), "hi 3");

        let res1 = parent.read_all(&buf)?;
        // check consistency
        for res in vec![res0, res1] {
            assert_eq!(res.len(), 40);
            assert_eq!(res.first().unwrap().id, 0);
            assert_eq!(res.last().unwrap().id, 39);
        }

        std::fs::remove_dir_all(buf)?;
        Ok(())
    }
}
