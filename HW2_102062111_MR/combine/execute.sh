#!/bin/bash

if [ $# -lt 2 ]; then
  echo "Please send \${input} and \${output}"
  exit 1
fi

function get_first() {
  echo $1
}

input=$1
output=$2
log=$(get_first $(echo $input | sed 's/\// /g'))
hdfs dfs -rm -r -f $output

hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output >> $log
retVal=$?
echo "[CURRENT STATE] Exit value = $retVal"
hdfs dfs -rm $output/_SUCCESS

exit $retVal
#hdfs dfs -mv $output/part-* $input/
