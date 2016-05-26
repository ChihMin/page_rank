if [ $# -lt 1 ]; then 
  echo "please input size : ./execute.sh \S{SIZE}"
  exit 1
fi

output=HW2_$1/iterator0C
input=HW2_$1/map_input

hdfs dfs -rm -r -f $output
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
