import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.uaparser.scala.Parser
import org.apache.spark.sql.functions._

import scala.util.Try

case class LogLine(line: String)
case class LogEntry(ip: String, bytes: Long, ua: String)
case class ThreeRegexMatches(first: String, second: String, third: String)
case class RequestStatistics(totalBytes: Long, requestCount: Int)
case class AggregatedLogEntry(ip: String, requestsStatistics: RequestStatistics)
case class ResultEntry(ip: String, meanBytes: Double, totalBytes: Long)

object BrowserKind extends Enumeration {
  val IE, FF, OTHER = Value
}

object BytesCounter {

  def main(args: Array[String]): Unit = {

    val logFile = args(0)
    val conf = new SparkConf()
                    .setAppName("lab1")

    val sc = new SparkContext(conf)

    val patternToExtractData = """^(\w+)\s[\w.-]+\s[\w.-]+\s\[.*\]\s".*"\s\d{3}\s(\d+|-)\s".*"\s"(.*)"$"""
    val accums = BrowserKind.values.toList.map(browser => browser -> sc.accumulator(0, browser.toString)).toMap

    val rawRdd = sc.textFile(logFile)
    val resultDf = processRdd(rawRdd, sc, accums, patternToExtractData)

    resultDf
      .coalesce(1) // we really don't want to end up with a gazillion of files
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(args(1))
    resultDf.show(5)

    accums.foreach(x => println(s"${x._2.name.get}: ${x._2.value}"))
  }

  def processRdd(rawRdd: RDD[String], sc: SparkContext, accums: Map[BrowserKind.Value, Accumulator[Int]],
                 pattern: String): DataFrame = {

    val sql = new SQLContext(sc)
    import sql.implicits._

    val logLines = convertRddToLogLinesDs(rawRdd, sql)
    val logEntries = logLines
      // ain't pretty, but necessary, since there's no encoder for Regex.Match
      .map(logLine => extractThreeMatchesFromString(logLine.line, pattern))
      .map(x => createLogEntryFromRegexMatches(x))
    val nonEmptyBytesLogEntries = logEntries.filter(x => x.bytes > 0).cache
    val result = nonEmptyBytesLogEntries
      .groupBy(x => x.ip)
      .mapGroups((key, logEntries) => groupLogEntries(key, logEntries))
      .map(x => createResultEntryFromAggregatedLogEntry(x))

    nonEmptyBytesLogEntries.map(logEntry => Parser.get.parse(logEntry.ua).userAgent.family.toLowerCase)
      .foreach(x => x match {
        case ("ie") => accums(BrowserKind.IE).add(1);
        case ("firefox") => accums(BrowserKind.FF).add(1);
        case (_) => accums(BrowserKind.OTHER).add(1);
      })

    // Spark Dataset API doesn't have any methods for ordering as of version 1.6.2
    result.toDF.orderBy(desc("totalBytes")).cache
  }

  def convertRddToLogLinesDs(rdd: RDD[String], sql: SQLContext): Dataset[LogLine] = {
    import sql.implicits._
    sql.createDataset(rdd).map(LogLine)
  }

  def extractThreeMatchesFromString(string: String, pattern: String): ThreeRegexMatches = {
    val regexp = pattern.r
    regexp.findAllMatchIn(string)
      .toList
      .take(1)
      .map(x => ThreeRegexMatches(x.group(1), x.group(2), x.group(3)))
      .head
  }

  def createLogEntryFromRegexMatches(matches: ThreeRegexMatches): LogEntry = {
    LogEntry(matches.first, matches.second
    match {
      case ("-") => 0L
      case (y) if parseLong(y).isDefined => y.toLong
      case (_) => throw new IllegalArgumentException("Second match should be convertible to Long or '-'")
    },
    matches.third)
  }

  def groupLogEntries(key: String, values: Iterator[LogEntry]): AggregatedLogEntry = {
    AggregatedLogEntry(key, values.map(x => RequestStatistics(x.bytes, 1))
                                  .reduce((a, b) => RequestStatistics(a.totalBytes + b.totalBytes,
                                                                        a.requestCount + b.requestCount)))
  }

  def createResultEntryFromAggregatedLogEntry(ale: AggregatedLogEntry): ResultEntry = {
    ResultEntry(ale.ip,
      (1.0 * ale.requestsStatistics.totalBytes) / ale.requestsStatistics.requestCount, ale.requestsStatistics.totalBytes)
  }

  def parseLong(s: String): Option[Long] = Try { s.toLong }.toOption
}