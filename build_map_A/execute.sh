output=HW2/map_input
input=/shared/HW2/sample-in/input-100M

hdfs dfs -rm -r ${output}
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
