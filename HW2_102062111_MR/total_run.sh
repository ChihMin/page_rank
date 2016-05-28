#!/bin/bash

if [ $# -lt 1 ]; then
  echo "please input size !"
  exit 1
fi

./build_graph.sh $1
./execute.sh $1
