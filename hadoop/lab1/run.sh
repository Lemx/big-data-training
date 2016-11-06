#!/usr/bin/env bash
path=${1-results/samples}
directory=`dirname ${path}`
filename=`basename ${path}`

hdfs dfs -rm -r /big-data-training/hadoop/lab1/input
hdfs dfs -rm -r /big-data-training/hadoop/lab1/output
hdfs dfs -mkdir -p /big-data-training/hadoop/lab1/input/ && \
hdfs dfs -put ${directory}/${filename} /big-data-training/hadoop/lab1/input/${filename} && \
mvn clean && \
mvn package && \
hadoop jar target/lab1-1.0.jar /big-data-training/hadoop/lab1/input/${filename} /big-data-training/hadoop/lab1/output && \
echo results of ${filename} && \
hdfs dfs -cat /big-data-training/hadoop/lab1/output/part-r-00000