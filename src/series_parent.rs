use crate::series_block::SeriesBlock;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct SeriesParent {
    /// when writing you always just look at the last block so thats simple stuff
    /// when reading you can rayon the blocks
    blocks: Vec<SeriesBlock>,
    max_points_per_block: u16,
}
