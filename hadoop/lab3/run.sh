#!/usr/bin/env bash
path=${1-data/000000}
directory=`dirname ${path}`
filename=`basename ${path}`

hdfs dfs -rm -r /big-data-training/hadoop/lab3/input
hdfs dfs -rm -r /big-data-training/hadoop/lab3/output
hdfs dfs -mkdir -p /big-data-training/hadoop/lab3/input/ && \
hdfs dfs -put ${directory}/${filename} /big-data-training/hadoop/lab3/input/${filename} && \
mvn clean && \
mvn package && \
hadoop jar target/lab3-1.0.jar /big-data-training/hadoop/lab3/input/${filename} /big-data-training/hadoop/lab3/output && \
echo results && \
hadoop fs -libjars target/lab3-1.0.jar -text /big-data-training/hadoop/lab3/output/part-r-00000