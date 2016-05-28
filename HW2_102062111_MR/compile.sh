#!/bin/bash

list=$(ls | grep -v .sh)
for folder in $list; do
  cd $folder
    pwd
    ./compile.sh
  cd -
done
