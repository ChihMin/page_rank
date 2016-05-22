output=HW2/iterator0
input=HW2/map_input

hdfs dfs -rm -r -f $output
hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage $input $output
# rm OUTPUT/*
# hdfs dfs -get $output/part-* OUTPUT/
