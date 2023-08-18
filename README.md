# tdb

## Is this you?

- I have a time constrained system I want to analyze.
- The most recent data is the most relevant: (1 | 2 | 3) days (1 | 2 | 3) weeks
- The time at which the data was written is useful information (optimized for queries in say past 1 hour, but you want this under the hood)
- You don't want to run a service.
- The file system should be the sync primitive for write read
- Experience should be made for having `tdb` on the vm and accessing it from your computer
- You just want to import a crate and have it work
- The data could be said to be a timeseries

  ^^ this is what we building

## Execution

- lets use [libflate](https://crates.io/crates/libflate) for file storage.
- files will be stored in `series`
- the points will be stored in 32 points per file (3600 * 24 / 32 = 2700 files per day if you write 1 per sec .. good enough, mby some reblocking on that front might be useful, have to see downstream), with a marked interval, being referenced in an `series-file`
- `series-file` also needs to be declaring the series
- should have an optional primary key for each series
- should be easy to query over ssh
