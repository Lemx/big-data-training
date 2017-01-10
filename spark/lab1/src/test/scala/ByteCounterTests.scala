import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class ByteCounterTests extends FlatSpec with Matchers {

  "A ByteCounter" should "convert RDD[String] into DataSet[LogLine]" in {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-lab1")
    val sc = new SparkContext(sparkConf)

    val sql = new SQLContext(sc)
    import sql.implicits._

    val arr = Array("foo", "bar", "baz")
    val expected = sql.createDataset(arr.map(LogLine))

    val rdd = sc.parallelize(arr)
    val ds = BytesCounter.convertRddToLogLinesDs(rdd, sql)

    ds.collect should be (expected.collect)

    sc.stop()
  }

  it should "extract three matches from a String" in {
    val str = Array(1, 2, 3).mkString(":")
    val pattern = "(.*):(.*):(.*)"
    val expected = ThreeRegexMatches("1", "2", "3")

    val matches = BytesCounter.extractThreeMatchesFromString(str, pattern)

    matches should be (expected)
  }

  it should "parse Longs" in {
    val str = "1234567890"

    val expected = Some(str.toLong)

    val long = BytesCounter.parseLong(str)

    long.get should be (expected.get)
  }

  it should "return None in case of unparsable Longs" in {
    val str = "-------"

    val expected = None

    val none = BytesCounter.parseLong(str)

    none should be (expected)
  }

  it should "convert matches to LogEntry" in {
    val matches = ThreeRegexMatches("1", "2", "3")

    val expected = LogEntry("1", 2L, "3")

    val logEntry = BytesCounter.createLogEntryFromRegexMatches(matches)

    logEntry should be (expected)
  }

  it should "convert matches to LogEntry with '-' second match" in {
    val matches = ThreeRegexMatches("1", "-", "3")

    val expected = LogEntry("1", 0L, "3")

    val logEntry = BytesCounter.createLogEntryFromRegexMatches(matches)

    logEntry should be (expected)
  }

  it should "throw IllegalArgumentException if second match unparsable and not '-'" in {
    val matches = ThreeRegexMatches("1", "foo", "3")
    a[IllegalArgumentException] should be thrownBy {
      BytesCounter.createLogEntryFromRegexMatches(matches)
    }
  }

  it should "aggregate grouped LogEntries into AggregatedLogEntries" in {
    val key = "foo"
    val values = List(LogEntry(key, 1, ""),
                      LogEntry(key, 2, ""),
                      LogEntry(key, 3, ""))
                  .iterator

    val expected = AggregatedLogEntry(key, RequestStatistics(6, 3))

    val agg = BytesCounter.groupLogEntries(key, values)

    agg should be (expected)
  }

  it should "convert AggregatedLogEntry into ResultEntry" in {
    val agg = AggregatedLogEntry("foo", RequestStatistics(6, 3))

    val expected = ResultEntry("foo", 2.0, 6)

    val res = BytesCounter.createResultEntryFromAggregatedLogEntry(agg)

    res should be (expected)
  }

  it should "process the RDD" in {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-lab1")
    val sc = new SparkContext(sparkConf)

    val accums = BrowserKind.values.toList.map(browser => browser -> sc.accumulator(0, browser.toString)).toMap
    val pattern = "(.*)#(.*)#(.*)"
    val sql = new SQLContext(sc)

    val arr = Array("foo#10#Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0",
                    "foo#30#Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0",
                    "bar#50#Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62",
                    "bar#-#Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62",
                    "baz#100#Opera/9.80 (Windows NT 5.1; U; en) Presto/2.6.30 Version/10.62",
                    "baz#150#Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))")
    val rdd = sc.parallelize(arr)

    val expected = sql.createDataFrame(Array(ResultEntry("baz", 125.0, 250L),
                                              ResultEntry("bar", 50.0, 50L),
                                              ResultEntry("foo", 20.0, 40L)))

    val resultDf = BytesCounter.processRdd(rdd, sc, accums, pattern)

    resultDf.collect should be (expected.collect)

    accums(BrowserKind.IE).value should be (1)
    accums(BrowserKind.FF).value should be (2)
    accums(BrowserKind.OTHER).value should be (2)

    sc.stop()
  }
}


