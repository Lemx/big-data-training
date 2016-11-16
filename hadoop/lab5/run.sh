#!/usr/bin/env bash
address=${1-localhost}
port=${2-55552}
path=${3-target/lab5-1.0.jar}
amcores=${4-1}
ammem=${5-256}
mem=${6-2048}
cores=${7-2}
containers=${8-1}
priority=${9-0}
iterations=${10-10000}
precision=${11-100000}


hdfs dfs -rm -r /big-data-training/hadoop/lab5/output
hdfs dfs -mkdir -p /big-data-training/hadoop/lab5/output/ && \
mvn clean && \
mvn package && \
hadoop jar ${path} ${amcores} ${ammem} ${path} ${port} && \
curl "http://${address}:${port}/run?memory=${mem}&cores=${cores}&containers=${containers}&priority=${priority}&iterations=${iterations}&precision=${precision}" && \
echo results && \
hadoop fs -cat /big-data-training/hadoop/lab5/output/result_1.txt