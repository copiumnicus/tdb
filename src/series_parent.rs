use crate::{
    fs::{read_or_create_default, template_and_mby_create, write},
    series_block::{SeriesBlock, SeriesBlockErr},
};
use anyhow::{anyhow, Result};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{hash::Hash, marker::PhantomData, path::PathBuf};

pub trait OwnsPrimaryKey<T> {
    fn get_primary_key(&self) -> T;
}

pub struct Series<T, K>
where
    K: PartialEq + Eq + Hash,
{
    buf: PathBuf,
    parent: SeriesParent<T, K>,
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

impl<T, K> Series<T, K>
where
    T: OwnsPrimaryKey<K> + Clone + Serialize + for<'a> Deserialize<'a>,
    K: PartialEq + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
{
    pub(crate) fn init_with_create_dir(
        higher_dir: PathBuf,
        series_name: impl ToString,
    ) -> Result<Self> {
        let series_name = fmt_key(series_name.to_string());
        let target = template_and_mby_create(&higher_dir, &series_name)?;
        let parent = read_or_create_default::<SeriesParent<T, K>>(&target, PARENT_NAME.as_str())?;
        Ok(Self {
            buf: target,
            parent,
        })
    }
    fn write_self(&self) -> Result<()> {
        write(&self.buf, PARENT_NAME.as_str(), &self.parent)
    }
    pub(crate) fn write(&mut self, point: T) -> Result<()> {
        // first write
        if self.parent.blocks.len() == 0 {
            let mut block = SeriesBlock::default();
            // don't handle `at capacity` err
            self.parent.index.insert(
                point.get_primary_key(),
                (
                    0, // block index
                    block
                        .write(&self.buf, &point, self.parent.max_points_per_block)
                        .map_err(|e| anyhow!("{:?}", e))?, // index within block
                ),
            );
            self.parent.blocks.push(block);
        } else {
            // nth write
            match self.parent.blocks.last_mut().unwrap().write(
                &self.buf,
                &point,
                self.parent.max_points_per_block,
            ) {
                Err(e) => match e {
                    SeriesBlockErr::BlockAtCapacity(_) => {
                        // populate next block
                        self.parent
                            .blocks
                            .push(self.parent.blocks.last().unwrap().next_block());
                        let block_index = self.parent.blocks.len() - 1;
                        // write to next block
                        self.parent.index.insert(
                            point.get_primary_key(),
                            (
                                block_index, // block index
                                self.parent
                                    .blocks
                                    .last_mut()
                                    .unwrap()
                                    .write(&self.buf, &point, self.parent.max_points_per_block)
                                    // don't handle the capacity again
                                    .map_err(|e| anyhow!("{:?}", e))?, // index within block
                            ),
                        );
                    }
                    SeriesBlockErr::Err(e) => return Err(e),
                },
                Ok(index_within) => {
                    self.parent.index.insert(
                        point.get_primary_key(),
                        (self.parent.blocks.len() - 1, index_within),
                    );
                }
            }
        }

        // update the parent declaration
        self.write_self()?;

        Ok(())
    }

    pub(crate) fn read_all(&self) -> Result<Vec<T>> {
        let result: Result<Vec<Vec<T>>, _> = self
            .parent
            .blocks
            .iter()
            .map(|block| block.read_all::<T>(&self.buf))
            .collect();
        Ok(result?.into_iter().flatten().collect())
    }

    pub(crate) fn read_by_key(&self, key: &K) -> Option<T> {
        let (block, inner) = self.parent.index.get(key)?.clone();
        let block = self.parent.blocks.get(block)?;
        let mut blocks = block.read_all::<T>(&self.buf).ok()?;
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
        let buf = PathBuf::from("./");

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

        let mut parent = Series::init_with_create_dir(buf.clone(), "test_parent")?;
        for id in 0..40 {
            parent.write(TestStruct {
                s: format!("hi {:?}", id),
                id,
            })?;
        }

        assert_eq!(parent.parent.blocks.len(), 2);

        let res0 = parent.read_all()?;
        // get parent fresh
        let parent = Series::<TestStruct, _>::init_with_create_dir(buf.clone(), "test_parent")?;

        assert_eq!(parent.parent.index.get(&3).unwrap().clone(), (0, 3));
        assert_eq!(parent.parent.index.get(&35).unwrap().clone(), (1, 3));

        assert_eq!(parent.read_by_key(&3).unwrap().s.as_str(), "hi 3");

        let res1 = parent.read_all()?;
        // check consistency
        for res in vec![res0, res1] {
            assert_eq!(res.len(), 40);
            assert_eq!(res.first().unwrap().id, 0);
            assert_eq!(res.last().unwrap().id, 39);
        }

        std::fs::remove_dir_all(buf.join("bc8d1c15e08802d934f9bdabc6d274b608ee31ad".to_string()))?;
        Ok(())
    }
}
