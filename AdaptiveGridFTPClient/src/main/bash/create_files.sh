#!/usr/bin/env bash

# arg1 = dir name
# arg2 = files name
# arg3 = start index
# arg4 = end index
# arg5 = seed
# arg6 = dump size

if [ -d "./$1" ]
then
    echo "Directory ./$1 exists."
else
    echo "Error: Directory /path/to/dir does not exists. Creating dir: $1"
    mkdir $1
fi

filename=$2
for ((i = $3; i <= $4; i++)); do
  filename=$2$i
  #echo $filename
  #     delay_time=$(((RANDOM%5)+1))
  #count_of_dump=$(((RANDOM % 2) + $5))
  #     sleep $delay_time
  count_of_dump=$5
  dd if=/dev/zero of=$PWD/$1/$filename bs=$6M count=$count_of_dump
  filename=$2
done

