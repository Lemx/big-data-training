#!/usr/bin/env bash
hadoopFs=${1-hdfs://localhost:8020/}
inpath=/big-data-training/hadoop/lab2/input
outpath=${2-/big-data-training/hadoop/lab2/output/bid_result.txt}
report=${3-report.csv}
outdir=`dirname ${outpath}`
outfile=`basename ${outpath}`

curl -OJ http://bunwell.cs.ucl.ac.uk/ipinyou.contest.dataset.zip && \
unzip ipinyou.contest.dataset.zip && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130606.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130607.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130608.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130609.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130610.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130611.txt.bz2 && \
bzip2 -dk ipinyou.contest.dataset/training2nd/bid.20130612.txt.bz2 && \
hdfs dfs -rm -r ${inpath}
hdfs dfs -rm -r ${outdir}
hdfs dfs -mkdir -p ${inpath} && \
hdfs dfs -mkdir -p ${outdir} && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130606.txt ${inpath}/bid.20130606.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130607.txt ${inpath}/bid.20130607.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130608.txt ${inpath}/bid.20130608.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130609.txt ${inpath}/bid.20130609.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130610.txt ${inpath}/bid.20130610.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130611.txt ${inpath}/bid.20130611.txt && \
hdfs dfs -put ipinyou.contest.dataset/training2nd/bid.20130612.txt ${inpath}/bid.20130612.txt && \
mvn clean && \
mvn package && \
pip install virtualenv && \
rm -r experiment
virtualenv experiment && \
source experiment/bin/activate && \
pip install -r requirements.txt && \
python runExperiment.py ${report} target/lab2-1.0.jar ${hadoopFs} ${inpath} ${outpath} && \
echo report && \
cat ${report}
