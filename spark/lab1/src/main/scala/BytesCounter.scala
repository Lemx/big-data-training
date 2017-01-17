import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.uaparser.scala.Parser

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
    val result = processRdd(rawRdd, sc, accums, patternToExtractData)

    result
      .coalesce(1) // we really don't want to end up with a gazillion of files
      .map(x => x._1 + "," + x._2._1 + "," + x._2._2)
      .saveAsTextFile(args(1))
    result.take(5).foreach(println)

    accums.foreach(x => println(s"${x._2.name.get}: ${x._2.value}"))
  }

  def processRdd(rawRdd: RDD[String], sc: SparkContext, accums: Map[BrowserKind.Value, Accumulator[Int]],
                 pattern: String): RDD[(String, (Double, Long))] = {

    val logEntries = rawRdd.map(x => extractThreeMatchesFromString(x, pattern))
    val nonEmptyBytesLogEntries = logEntries.filter(x => x._2._1 !=  "-").cache
    val result = nonEmptyBytesLogEntries.map(convertToCountingTuple)
                                        .reduceByKey(aggregateEntries)
                                        .map(x => calculateMean(x))

    nonEmptyBytesLogEntries.map(x => Parser.get.parse(x._2._2).userAgent.family.toLowerCase)
      .foreach(x => x match {
        case ("ie") => accums(BrowserKind.IE).add(1);
        case ("firefox") => accums(BrowserKind.FF).add(1);
        case (_) => accums(BrowserKind.OTHER).add(1);
      })

    result.sortBy(x => x._2._2, ascending = false)
  }

  def extractThreeMatchesFromString(string: String, pattern: String): (String, (String, String)) = {
    val regexp = pattern.r
    val regexp(ip, bytes, ua) = string
    (ip, (bytes, ua))
  }

  def convertToCountingTuple(value: (String, (String, String))): (String, (Long, Long)) = {
    (value._1, (value._2._1.toLong, 1L))
  }

  def aggregateEntries(value1: (Long, Long), value2: (Long, Long)): (Long, Long) = {
    (value1._1 + value2._1, value1._2 + value2._2)
  }

  def calculateMean(value: (String, (Long, Long))): (String, (Double, Long)) = {
    (value._1, ((1.0 * value._2._1) / value._2._2, value._2._1))
  }
}