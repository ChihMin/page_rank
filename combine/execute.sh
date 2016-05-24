#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Please send \${input} and \${output}"
  exit 1
fi

input=$1
output=$2
hdfs dfs -rm -r -f $output
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
retVal=$?
echo "[CURRENT STATE] Exit value = $retVal"
hdfs dfs -rm $output/_SUCCESS

exit $retVal
#hdfs dfs -mv $output/part-* $input/
