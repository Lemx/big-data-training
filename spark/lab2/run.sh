#!/usr/bin/env bash
pathToSparkSubmit=$1

if [ -z ${pathToSparkSubmit} ];
    then echo "path to spark-submit isn't set, quitting";
    exit
fi

flights="2007.csv"
carriers="carriers.csv"
airports="airports.csv"

curl -OJ http://stat-computing.org/dataexpo/2009/2007.csv.bz2 && \
bzip2 -dk 2007.csv.bz2 && \
curl -OJ http://stat-computing.org/dataexpo/2009/carriers.csv && \
curl -OJ http://stat-computing.org/dataexpo/2009/airports.csv && \

sbt assembly && \
sh ${pathToSparkSubmit} target/scala-2.10/lab2-assembly-1.0.jar ${flights} ${carriers} ${airports}