#!/usr/bin/env bash
pathToSparkSubmit=$1
inPath=${2-data/000000}
outPath=${3-results}

if [ -z ${pathToSparkSubmit} ];
    then echo "path to spark-submit isn't set, quitting";
    exit
fi

sbt assembly && \
sh ${pathToSparkSubmit} target/scala-2.10/lab1-assembly-1.0.jar ${inPath} ${outPath}