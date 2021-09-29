# Instructions 

Follow the steps below to merge and then extract the input csv file from the archives.

1) Merge the part1, part2, part3 files: 

$ cat input.tar.gz.part1 input.tar.gz.part2 input.tar.gz.part3 > input.tar.gz

2) Once merged, extract the csv file from the archive:

$ tar -xvf input.tar.gz 
