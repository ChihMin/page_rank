if [ $# -lt 1 ]; then 
  echo "please input size : ./execute.sh \S{SIZE}"
  exit 1
fi

output=HW2_$1/map_input
input=HW2_$1/edge_filter

hdfs dfs -rm -r ${output}
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
