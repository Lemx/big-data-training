#!/usr/bin/env bash
connection=${1-jdbc:hive2://}
curl -OJ http://stat-computing.org/dataexpo/2009/2007.csv.bz2
bzip2 -dk 2007.csv.bz2
curl -OJ http://stat-computing.org/dataexpo/2009/carriers.csv
curl -OJ http://stat-computing.org/dataexpo/2009/airports.csv
hdfs dfs -mkdir -p /big-data-training/hive/lab1/input
hdfs dfs -put *.csv /big-data-training/hive/lab1/input

beeline -u ${1} -f init.q
beeline -u ${1} -f query.q