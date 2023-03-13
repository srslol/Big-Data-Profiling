https://datasketches.apache.org/

For work, I was tasked with finding a way to approximate large data sets in S3 buckets measured in terabytes.  They wanted to pull the data once, get a profile of the data (min/max/data types/rows/quantiles/distinct counts/samples/most frequent/etc).  I found this would work best so I used Maven Java and this was used for benchmarking against other methods that proved to be less effective.  
