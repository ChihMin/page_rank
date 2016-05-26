if [ $# -lt 1 ]; then 
  echo "please input size : ./execute.sh \S{SIZE}"
  exit 1
fi

output=HW2_$1/map_input
input=/shared/HW2/sample-in/input-$1

hdfs dfs -rm -r ${output}
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
