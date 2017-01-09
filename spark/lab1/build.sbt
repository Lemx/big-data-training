lazy val root = (project in file(".")).
  settings (
    name := "lab1",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("BytesCounter")
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.uaparser" % "uap-scala_2.10" % "0.1.0"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

parallelExecution in Test := false