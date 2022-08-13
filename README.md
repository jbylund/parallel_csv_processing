# Parallel CSV Processing

This repo provides a super simple demonstration of processing a large csv using multiple processes.
This works by:

1. partitioning the csv into chunks
1. applying the transformation to each chunk
1. re-combining the chunks to produce the output csv

This relies on the splitting and merging operations being very fast relative the transformation (which for many non-trivial transformations is the case).
