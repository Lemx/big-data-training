#!/usr/bin/env bash
pathToSparkSubmit=$1
objects=${2-data/Objects.csv}
target=${3-data/Target.csv}
rocPath=${4-result}

if [ -z ${pathToSparkSubmit} ];
    then echo "path to spark-submit isn't set, quitting";
    exit
fi

sbt assembly && \
sh ${pathToSparkSubmit} target/scala-2.10/lab3-assembly-1.0.jar ${objects} ${target} ${rocPath}
