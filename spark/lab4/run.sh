#!/usr/bin/env bash
pathToSparkSubmit=$1
connection=${2--u "jdbc:hive2://localhost:10000" -n hive -p hive}
interface=${3-en0}
thrift=${4-thrift://localhost:9083}
kafka=${5-localhost:9092}
hdfs=${6-hdfs://localhost:8020/}

if [ -z ${pathToSparkSubmit} ];
    then echo "path to spark-submit isn't set, quitting";
    exit
fi


sbt assembly && \
hdfs dfs -mkdir -p /big-data-training/spark/lab4/stats && \
beeline ${connection} -f init.q && \
sh ${pathToSparkSubmit} target/scala-2.10/lab4-assembly-1.0.jar ${interface} ${thrift} ${kafka} ${hdfs}
