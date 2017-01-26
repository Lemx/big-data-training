lazy val root = (project in file(".")).
  settings (
    name := "lab4",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("PacketReceiver")
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"
libraryDependencies += "org.pcap4j" % "pcap4j-packetfactory-static" % "1.7.0"
libraryDependencies += "org.pcap4j" % "pcap4j-core" % "1.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3" excludeAll ExclusionRule(organization = "javax.servlet")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

parallelExecution in Test := false