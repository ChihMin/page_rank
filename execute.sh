#!/bin/bash


i=1
EXEC=./execute.sh
pre=HW2
while [ $i -lt 22 ]; do
  j=$(($i-1))
  
  input=$pre/iterator${j}C
  output=$pre/iterator${i}
  cd iteration_A
    $EXEC $input $output
  cd -

  input=$pre/iterator${i}
  output=$pre/iterator${i}B
  cd iteration_B
    $EXEC $input $output
  cd -

  input=$pre/iterator${i}B
  output=$pre/iterator${i}C
  cd iteration_C
    $EXEC $input $output
  cd -
  
  input=$pre/iterator${i}C
  output=$pre/result
  cd combine
    $EXEC $input $output
    if [ $? -eq 255 ]; then
      echo "success iteration in $i times"
      exit 0
    fi
  cd -
  
  i=$(($i+1))
done

# hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
