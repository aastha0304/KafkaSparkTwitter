package kafkasparkwindows

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import java.nio.ByteBuffer
import scala.util.Random
import org.apache.spark.streaming.dstream._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage._
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
/**
  * Created by nik on 1/2/18.
  */
object KafkaSparkWindows {
  val topics = "twitterstream"    // command separated list of topics
  val brokers = "localhost:29092"   // comma separated list of broker:host
  val windowSize = Duration(30000L)          // 10 seconds
  val slidingInterval = Duration(10000L)      // 2 seconds
  val checkpointInterval = Duration(20000L)  // 20 seconds
  val batchIntervalSeconds = 2
  val checkpointDir = "file:///tmp"

  def main(args: Array[String]) {
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    ssc.start()

    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)
  }

  def creatingFunc(): StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaSparkWindows")
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(batchIntervalSeconds))

    val wordStream = createKafkaStream(ssc).map(x => (x, 1))
    val runningCountStream = wordStream.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))

    runningCountStream.checkpoint(checkpointInterval)

    runningCountStream.print()
    val sqlContext = SparkSession
      .builder()
      .getOrCreate().sqlContext
    import sqlContext.implicits._
    runningCountStream.foreachRDD { rdd =>
      sqlContext.createDataFrame(rdd).toDF("word", "count").createOrReplaceTempView("tweet")
      rdd.take(1)
      rdd.map(row => parser(row._2).saveAsTextFile("file:///tmp/")
    }

    ssc.remember(Minutes(1))

    ssc.checkpoint(checkpointDir)

    println("Creating function called to create new StreamingContext")
    ssc
  }

  def parser(json: String): String = {
    import scala.util.parsing.json.JSON
    JSON.parseFull(json).get.asInstanceOf[Map[String, Any]].get("text").toString
  }

  def createKafkaStream(ssc: StreamingContext) = { //: DStream[(String, String)] = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:29092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    .map(record => (record.key, record.value))
  }
}
