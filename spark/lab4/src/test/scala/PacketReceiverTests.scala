import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class PacketReceiverTests extends FlatSpec with BeforeAndAfter with Matchers {

  var sc: SparkContext = _

  before {
    System.setProperty("HADOOP_HOME", "D:\\hadoop")
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-lab4")
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop
  }

  "A PacketReceiver" should "extract Settings from single-row RDD" in {
    val rdd = sc.parallelize(Seq((null, 1, 5.0, 1L))).map(x => Row.fromTuple(x))
    val expected = Settings(null, 1, 5.0, 1L)
    val default = Settings("1.1.1.1", 1, 2.0, 3L)
    val settings = PacketReceiver.extractSettings(rdd, default)

    settings should be (expected)
  }

  it should "replace invalid Settings fields with default values in case of any issues" in {
    val multiRowRdd = sc.parallelize(Seq(("", "", "", ""), ("", "", "", ""))).map(x => Row.fromTuple(x))
    val rddWithBadIp = sc.parallelize(Seq(("", 1, 5.0, 1L))).map(x => Row.fromTuple(x))
    val rddWithBadType = sc.parallelize(Seq((null, "5", 5.0, 1L))).map(x => Row.fromTuple(x))
    val rddWithBadValue = sc.parallelize(Seq((null, 1, "foo", 1L))).map(x => Row.fromTuple(x))
    val rddWithBadPeriod = sc.parallelize(Seq((null, 1, 5.0, "bar"))).map(x => Row.fromTuple(x))

    val default = Settings(null, 1, 5.0, 1L)
    val expected = Settings(null, 1, 5.0, 1L)

    val multirowReplacedWithDefault = PacketReceiver.extractSettings(multiRowRdd, default)
    val ipReplaced = PacketReceiver.extractSettings(rddWithBadIp, default)
    val typeReplaced = PacketReceiver.extractSettings(rddWithBadType, default)
    val valueReplaced = PacketReceiver.extractSettings(rddWithBadValue, default)
    val periodReplaced = PacketReceiver.extractSettings(rddWithBadPeriod, default)

    multirowReplacedWithDefault should be (expected)
    ipReplaced should be (expected)
    typeReplaced should be (expected)
    valueReplaced should be (expected)
    periodReplaced should be (expected)
  }

  it should "determine event type based on value, limit and monitoring type" in {
    val expectedThresholdNorm = EventType.ThresholdNorm
    val expectedThresholdExceeded = EventType.ThresholdExceeded
    val expectedLimitNorm = EventType.LimitNorm
    val expectedLimitExceeded = EventType.LimitExceeded

    val tNorm = PacketReceiver.getEventType(1, 2, 1)
    val tExc = PacketReceiver.getEventType(2, 1, 1)
    val lNorm = PacketReceiver.getEventType(1, 2, 2)
    val lExc = PacketReceiver.getEventType(2, 1, 2)

    tNorm should be (expectedThresholdNorm)
    tExc should be (expectedThresholdExceeded)
    lNorm should be (expectedLimitNorm)
    lExc should be (expectedLimitExceeded)
  }

  it should "determine whether event type for ip/type combination has changed since last check" in {
    val expected = true
    val changed = PacketReceiver.checkIfChanged(1, "foo", EventType.LimitExceeded)

    changed should be (expected)

    val expectedNotChanged = false
    val notChanged = PacketReceiver.checkIfChanged(1, "foo", EventType.LimitExceeded)

    notChanged should be (expectedNotChanged)

    val expectedChangedAgain = true
    val changedAgain = PacketReceiver.checkIfChanged(1, "foo", EventType.LimitNorm)

    changedAgain should be (expectedChangedAgain)
  }

  it should "compute values based on monitoring type" in {
    val expectedValueThresholdType = 10.0
    val valueThresholdType = PacketReceiver.aggregationByType(1)(100, 10)

    valueThresholdType should be (expectedValueThresholdType)

    val expectedValueLimitType = 100.0
    val valueLimitType = PacketReceiver.aggregationByType(2)(100, 10)

    valueLimitType should be (expectedValueLimitType)
  }

  it should "compose strings for kafka" in {
    val id = UUID.randomUUID.toString
    val time = DateTime.now
    val ip = "foo"
    val eventType = EventType.LimitNorm
    val value = 10.0
    val limit = 40.0
    val period = 100L
    val expected = s"$id\t$time\t$ip\t$eventType\t$value\t$limit\t$period"
    val result = PacketReceiver.getStringForKafka(id, time, ip, eventType, value, limit, period)

    result should be (expected)
  }
}
