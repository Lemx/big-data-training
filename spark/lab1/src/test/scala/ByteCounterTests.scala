import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

class ByteCounterTests extends FlatSpec with Matchers {

  "A ByteCounter" should "extract three matches from a String" in {
    val str = Array(1, 2, 3).mkString(":")
    val pattern = "(.*):(.*):(.*)"
    val expected = ("1", ("2", "3"))

    val matches = BytesCounter.extractThreeMatchesFromString(str, pattern)

    matches should be (expected)
  }

  it should "convert (String, (String, String)) to (String, (Long, Long))" in {
    val matches = ("1", ("2", "3"))

    val expected = ("1", (2L, 1L))

    val converted = BytesCounter.convertToCountingTuple(matches)

    converted should be (expected)
  }


  it should "aggregate two (Long, Long) into single (Long, Long)" in {
    val key = "foo"
    val val1 = (50L, 2L)
    val val2 = (5L, 1L)

    val expected = (55L, 3L)

    val agg = BytesCounter.aggregateEntries(val1, val2)

    agg should be (expected)
  }

  it should "compute mean and total from (String, (Long, Long))" in {
    val agg = ("foo", (6L, 3L))

    val expected = ("foo", (2.0D, 6L))

    val res = BytesCounter.calculateMean(agg)

    res should be (expected)
  }

  it should "process the RDD and count browsers" in {
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

    val expected = sc.parallelize(Array(("baz", (125.0D, 250L)), ("bar", (50.0D, 50L)), ("foo", (20.0D, 40L))))

    val resultDf = BytesCounter.processRdd(rdd, sc, accums, pattern)

    resultDf.collect should be (expected.collect)

    accums(BrowserKind.IE).value should be (1)
    accums(BrowserKind.FF).value should be (2)
    accums(BrowserKind.OTHER).value should be (2)
  }
}


