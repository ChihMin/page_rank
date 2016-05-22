#!/bin/bash


i=1
while [ $i -lt 2 ]; do
  j=$(($i-1))
  input=HW2/iterator$j
  output=HW2/iterator$i
  echo "$input $output"

  hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output

  i=$(($i+1))
done

# hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
