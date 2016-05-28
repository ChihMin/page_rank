#!/bin/bash

if [ $# -lt 1 ]; then
  echo "Please enter size !"
  exit 1
fi

list="A B C"

for chr in $list; do
  cd build_map_$chr
    ./execute.sh $1
  cd -
done
