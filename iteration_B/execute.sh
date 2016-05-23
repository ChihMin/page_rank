#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Please send \${input} and \${output}"
  exit 1
fi

input=$1
output=$2
hdfs dfs -rm -r -f $output
hdfs dfs -rm $input/part-*
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
hdfs dfs -rm $output/_SUCCESS
hdfs dfs -mv $output/part-* $input/
