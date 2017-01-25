####Requirements:  
- sbt 0.13.6+  
- Spark 1.6.2
- Kafka 0.10.1.0
- Hive 2.1.0


####Instructions
Execute run.sh with parameters:  
- path to spark-submit script  
- (optional) beeline JDBC connection args  
- (optional) name of the interface to listen on  
- (optional) URI of hive thrift metastore  
- (optional) address of Kafka Bootstrap listener  
- (optional) URI of HDFS root

####Warning
should be run with root priveleges (pcap donesn't like user mode)