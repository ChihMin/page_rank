#!/bin/bash

spark-submit  --class WordCount \
              --num-executors 30 \
              target/scala-2.10/wordcount-application_2.10-1.0.jar
test -e output || mkdir output
rm  output/*
hdfs dfs -get HW2_Spark_100M/* output/
