#!/usr/bin/env bash
connection=${1--u "jdbc:hive2://localhost:10000" -n hive -p hive}
curl -OJ http://stat-computing.org/dataexpo/2009/2007.csv.bz2 && \
bzip2 -dk 2007.csv.bz2 && \
curl -OJ http://stat-computing.org/dataexpo/2009/carriers.csv && \
curl -OJ http://stat-computing.org/dataexpo/2009/airports.csv && \
hdfs dfs -rm -r /big-data-training/hive/lab2/input
hdfs dfs -mkdir -p /big-data-training/hive/lab2/input && \
hdfs dfs -put *.csv /big-data-training/hive/lab2/input && \

beeline ${connection} -f init.q && \
beeline ${connection} -f query.q