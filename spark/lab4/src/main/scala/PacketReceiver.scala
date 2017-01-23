import java.sql.Timestamp
import java.util
import java.util.UUID

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode
import org.pcap4j.core.{PacketListener, PcapHandle, Pcaps}
import org.pcap4j.packet.{IpV4Packet, Packet}

import scala.collection.mutable

case class PacketInfo(ip: String, length: Long)

object EventType extends Enumeration {
  val ThresholdExceeded, ThresholdNorm, LimitExceeded, LimitNorm = Value
}

object KafkaSink {
  def apply(config: util.Map[String, Object]): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
}

object PacketReceiver {

  val aggregationByType: Map[Int, (Long, Long) => Double] = Map((1, (a, b) => (1.0 * a) / b), (2, (a, _) => a))
  val eventsCache = mutable.HashMap.empty[String, EventType.Value]

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf()
      .setAppName("lab4")
    System.setProperty("hive.metastore.uris", "thrift://localhost:9083") // not sure about that, but it works
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("USE lab4")
    val settings = hiveContext.sql("SELECT * FROM settings LIMIT 1").cache
    val hostIp = settings.select($"hostIp").take(1)(0)(0).asInstanceOf[String]
    val monitoringType = settings.select($"type").take(1)(0)(0).asInstanceOf[Int]
    val limit = settings.select($"value").take(1)(0)(0).asInstanceOf[Double]
    val period = settings.select($"period").take(1)(0)(0).asInstanceOf[Long]

    val mb = 1024 * 1024

    val interfaceName = args.length match {
      case 0 => "en0"
      case 1 => args(0)
    }
    val ssc = new StreamingContext(sc, Seconds(1))
    val kafkaConfig = new util.HashMap[String, Object]
    kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaSink = sc.broadcast(KafkaSink(kafkaConfig))
    ssc.checkpoint("checkpoints")

    val packets = ssc.receiverStream(new PacketReceiver(interfaceName))

    packets.reduceByKeyAndWindow((a, b) => a + b, (a, b) => a - b, Seconds(period), Seconds(1))
      .filter(x => x._1 == hostIp || hostIp == null)
      .map(x => (x._1, aggregationByType(monitoringType)(x._2, period)))
      .map(x => (x._1, x._2, getEventType(x._2, limit * mb, monitoringType)))
      .filter(x => checkIfChanged(x._1, x._3))
      .foreachRDD(rdd =>
        rdd.foreachPartition(part =>
          part.foreach(x =>
            kafkaSink.value.send("alerts", getStringForKafka(x._1, x._3, x._2 / mb, limit, period)))))

    packets.reduceByKeyAndWindow((a, b) => a + b, (a, b) => a - b,  Minutes(60), Minutes(60))
      .map(x => (new Timestamp(DateTime.now.getMillis), x._1, (1.0 * x._2) / mb, (1.0 * x._2 / mb) / 60))
      .foreachRDD(rdd => {
        val hive = new HiveContext(rdd.sparkContext)
        hive.sql("USE lab4")

        val df = hive.createDataFrame(rdd)
        df.registerTempTable("stats")
        val stats = hive.sql("INSERT INTO TABLE statistics_by_hour SELECT * FROM stats")
        stats.write.mode(SaveMode.Append).saveAsTable("statistics_by_hour")
          })

    ssc.start()
    ssc.awaitTermination()
  }

  def getEventType(value: Double, limit: Double, monitoringType: Int): EventType.Value = {
    monitoringType match {
      case 1 => if (value > limit) EventType.ThresholdExceeded else EventType.ThresholdNorm
      case 2 => if (value > limit) EventType.LimitExceeded else EventType.LimitNorm
    }
  }

  def checkIfChanged(ip: String, eventType: EventType.Value): Boolean = {
    if (eventsCache.contains(ip) && eventsCache(ip) == eventType) {
      false
    }
    else {
      eventsCache(ip) = eventType
      true
    }
  }

  def getStringForKafka(ip: String, eventType: EventType.Value, value: Double, limit: Double, period: Long): String = {
    List(UUID.randomUUID, DateTime.now, ip, eventType, value, limit, period).mkString("\t")
  }
}


class PacketReceiver(interface: String) extends Receiver[(String, Long)](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var handle:PcapHandle = _

  def onStart() {
    new Thread("PacketReceiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def receive() {
    val nif = Pcaps.getDevByName(interface)

    val maxPacketLen = 64 * 1024
    val mode = PromiscuousMode.NONPROMISCUOUS //we don't need anything that shouldn't have come here anyway
    val timeout = 1000
    handle = nif.openLive(maxPacketLen, mode, timeout)

    val listener = new PacketListener() {
      def gotPacket(packet: Packet) {
        if (packet != null) {
          val ipPacket = packet.get(classOf[IpV4Packet])
          if (ipPacket != null) {
            val header = ipPacket.getHeader
            val ip = header.getSrcAddr.getHostAddress
            val len = header.getTotalLengthAsInt.toLong
            store((ip, len))
          }
        }
      }
    }
    handle.loop(-1, listener)
  }

  def onStop(): Unit = {
    handle.breakLoop()
    handle.close()
  }
}
