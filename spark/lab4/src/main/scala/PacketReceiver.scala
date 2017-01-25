import java.io.{BufferedWriter, OutputStreamWriter}
import java.sql.Timestamp
import java.util
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.joda.time.DateTime
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode
import org.pcap4j.core.{PacketListener, PcapHandle, Pcaps}
import org.pcap4j.packet.{IpV4Packet, Packet}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class Settings(ip: String, monitoringType: Int, limit: Double, period: Long)

object EventType extends Enumeration {
  val ThresholdExceeded, ThresholdNorm, LimitExceeded, LimitNorm = Value
}

object PacketReceiver {

  val mb = 1024.0 * 1024.0
  val aggregationByType: Map[Int, (Long, Long) => Double] = Map(
    (1, (a, b) => (1.0 * a) / b),
    (2, (a, _) => a)
  )
  val eventsCache = mutable.HashMap((1, mutable.HashMap.empty[String, EventType.Value]),
                                    ((2, mutable.HashMap.empty[String, EventType.Value])))

  def main(args: Array[String]): Unit = {

    val Array(interface, metastore, kafka, fs) = args.length match {
      case 4 => args
      case (v) if v > 4 => {
        println("extra args found, using first four")
        args.slice(0, 4)
      }
      case (v) if v < 4 => {
        println("not all args are set, falling back to defaults")
        Array("en0", "thrift://localhost:9083", "localhost:9092", "hdfs://localhost:8020/")
      }
    }

    val sparkConf = new SparkConf()
      .setAppName("lab4")
      .setMaster("local[*]")
    System.setProperty("hive.metastore.uris", metastore) // not sure about that, but it works
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    hiveContext.sql("USE lab4")
    val settings = hiveContext.sql("SELECT * FROM settings").cache
    val thresholdSettingsRow = settings.where($"type" === 1)
    val limitSettingsRow = settings.where($"type" === 2)

    val thresholdSettings = extractSettings(thresholdSettingsRow, Settings(null, 1, 5, 10))
    val limitSettings = extractSettings(limitSettingsRow, Settings(null, 2, 30, 60))

    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("checkpoints")

    val kafkaConfig = getKafkaConfig(kafka)
    val kafkaSink = sc.broadcast(KafkaSink(kafkaConfig))

    val packets = ssc.receiverStream(new PacketReceiver(interface))

    monitorPackets(thresholdSettings, packets, kafkaSink.value)
    monitorPackets(limitSettings, packets, kafkaSink.value)
    storePacketsStats(packets, fs)

    ssc.start()
    ssc.awaitTermination()
  }

  private def extractSettings(settingsRow: DataFrame, default: Settings): Settings = {
    val thresholdSettings = settingsRow.count match {
      case 1 => {
        val raw = settingsRow.take(1)(0)
        val ip = Try(raw.getAs[String](0)) match {
          case Success(v) => v
          case Failure(_) => default.ip
        }
        val monitoringType = default.monitoringType
        val limit = Try(raw.getAs[Double](2)) match {
          case Success(v) => v
          case Failure(_) => default.limit
        }
        val period = Try(raw.getAs[Long](3)) match {
          case Success(v) => v
          case Failure(_) => default.period
        }
        Settings(ip, monitoringType, limit, period)
      }
      case _ => default
    }
    thresholdSettings
  }

  private def getKafkaConfig(kafkaBootstrap: String): util.HashMap[String, Object] = {
    val kafkaConfig = new util.HashMap[String, Object]
    kafkaConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
    kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaConfig
  }

  def monitorPackets(settings: Settings, packets: ReceiverInputDStream[(String, Long)], sink: KafkaSink): Unit = {
    packets.reduceByKeyAndWindow((a, b) => a + b, (a, b) => a - b, Seconds(settings.period), Seconds(1))
      .filter(x => x._1 == settings.ip || settings.ip == null)
      .map(x => (x._1, aggregationByType(settings.monitoringType)(x._2, settings.period)))
      .map(x => (x._1, x._2, getEventType(x._2, settings.limit * mb, settings.monitoringType)))
      .filter(x => checkIfChanged(settings.monitoringType, x._1, x._3))
      .foreachRDD(rdd =>
        rdd.foreachPartition(part =>
          part.foreach(x =>
            sink.send("alerts", getStringForKafka(UUID.randomUUID.toString, x._1, x._3, x._2 / mb, settings.limit, settings.period)))))
  }

  def getEventType(value: Double, limit: Double, monitoringType: Int): EventType.Value = {
    monitoringType match {
      case 1 => if (value > limit) EventType.ThresholdExceeded else EventType.ThresholdNorm
      case 2 => if (value > limit) EventType.LimitExceeded else EventType.LimitNorm
    }
  }

  def checkIfChanged(monitoringType:Int, ip: String, eventType: EventType.Value): Boolean = {
    if (eventsCache.contains(monitoringType)
        && eventsCache(monitoringType).contains(ip)
        && eventsCache(monitoringType)(ip) == eventType) {
      false
    }
    else {
      eventsCache(monitoringType)(ip) = eventType
      true
    }
  }

  def getStringForKafka(id: String, ip: String, eventType: EventType.Value, value: Double, limit: Double, period: Long): String = {
    List(id, DateTime.now, ip, eventType, value, limit, period).mkString("\t")
  }

  private def storePacketsStats(packets: ReceiverInputDStream[(String, Long)], defaultFS: String): Unit = {
    packets.reduceByKeyAndWindow((a, b) => a + b, (a, b) => a - b, Minutes(60), Minutes(60))
      .map(x => (new Timestamp(DateTime.now.getMillis), x._1, (1.0 * x._2) / mb, (1.0 * x._2 / mb) / 60))
      .map(x => s"${x._1}, ${x._2}, ${x._3}, ${x._4}${System.getProperty("line.separator")}")
      .foreachRDD(rdd =>
        rdd.foreachPartition(part => {

          val conf = new Configuration()
          conf.set("fs.defaultFS", defaultFS)
          conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
          conf.set("fs.file.impl", classOf[LocalFileSystem].getName)

          val fs = FileSystem.get(conf)
          val outputStream = fs.create(new Path("/big-data-training/spark/lab4/stats/" + UUID.randomUUID))
          val br = new BufferedWriter(new OutputStreamWriter(outputStream))
          part.foreach(x => br.write(x))
          br.close()
          fs.close()
        })
      )
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