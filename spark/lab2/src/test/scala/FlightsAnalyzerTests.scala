import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.functions._

class FlightsAnalyzerTests extends FlatSpec with BeforeAndAfter with Matchers {

  var sc: SparkContext = _

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-lab2")
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop
  }

  "A FlightsAnalyzer" should "perform inner join on DataFrames" in {
    val sql = new SQLContext(sc)
    import sql.implicits._

    val left = sc.parallelize(Seq(("foo", 1, 2), ("bar", 3, 4), ("baz", 5, 6))).toDF("key", "int1", "int2")
    val right = sc.parallelize(Seq(("foo", true), ("foo", false), ("bar", true))).toDF("name", "bool")

    val expected = sc.parallelize(Seq(("foo", 1, 2, "foo", true), ("foo", 1, 2, "foo", false), ("bar", 3, 4, "bar", true)))
                      .toDF("key", "int1", "int2", "name", "bool").orderBy("key")

    val joined = FlightsAnalyzer.innerJoinDFs(left, "key", right, "name").orderBy("key")

    joined.collect should be (expected.collect)
  }

  it should "group by column, count and order by count desc" in {
    val sql = new SQLContext(sc)
    import sql.implicits._

    val df = sc.parallelize(Seq(("foo", 1), ("foo", 3), ("bar", 5), ("foo", 10), ("bar", 22))).toDF("key", "int")

    val expected = sc.parallelize(Seq(("foo", 3), ("bar", 2))).toDF("key", "count")

    val joined = FlightsAnalyzer.groupByColumnCountOrderByCount(df, "key", desc)

    joined.collect should be (expected.collect)
  }

  it should "group by column, count and order by count asc" in {
    val sql = new SQLContext(sc)
    import sql.implicits._

    val df = sc.parallelize(Seq(("foo", 1), ("foo", 1), ("bar", 1), ("foo", 1), ("bar", 2))).toDF("key", "int")

    val expected = sc.parallelize(Seq((2, 1), (1, 4))).toDF("int", "count")

    val joined = FlightsAnalyzer.groupByColumnCountOrderByCount(df, "int", asc)

    joined.collect should be (expected.collect)
  }

  it should "perform filtering on DataFrame with 'in' and 'where' at the same time" in {
    val sql = new SQLContext(sc)
    import sql.implicits._

    val df = sc.parallelize(Seq(("foo", 1, true),
                                ("foo", 2, true),
                                ("foo", 3, true),
                                ("bar", 2, true),
                                ("foo", 1, false),
                                ("bar", 2, false)))
                .toDF("key", "int", "bool")

    val expected = sc.parallelize(Seq(("foo", 1, true),
                                      ("foo", 2, true),
                                      ("bar", 2, true)))
                .toDF("key", "int", "bool")

    val filtered = FlightsAnalyzer.filter(df, Seq(("int", Seq(1, 2)), ("bool", true)))

    filtered.collect should be (expected.collect)
  }
}
