#!/usr/bin/env bash
inpath=/big-data-training/hadoop/lab4/input
outpath=/big-data-training/hadoop/lab4/output
auxpath=/big-data-training/hadoop/lab4/aux/city.en.txt

outdir=`dirname ${outpath}`
outfile=`basename ${outpath}`

auxdir=`dirname ${auxpath}`
auxfile=`basename ${auxpath}`

curl -OJ http://bunwell.cs.ucl.ac.uk/ipinyou.contest.dataset.zip && \
unzip ipinyou.contest.dataset.zip && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131019.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131020.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131021.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131022.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131023.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131024.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131025.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131026.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training3rd/imp.20131027.txt.bz2 && \
hdfs dfs -rm -r ${inpath}
hdfs dfs -rm -r ${outpath}
hdfs dfs -rm -r ${auxdir}
hdfs dfs -mkdir -p ${inpath} && \
hdfs dfs -mkdir -p ${auxdir} && \
hdfs dfs -put ipinyou.contest.dataset/training3rd/*.txt ${inpath} && \
hdfs dfs -put ipinyou.contest.dataset/${auxfile} ${auxpath} && \
mvn clean && \
mvn package && \
hadoop jar target/lab4-1.0.jar ${inpath} ${auxpath} ${outpath} && \
echo results && \
hdfs dfs -ls ${outpath}