lazy val root = (project in file(".")).
  settings (
    name := "lab3",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("ResponseAnalyzer")
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.2"
libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.12"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}