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
