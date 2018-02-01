package kafkasparksql

/**
  * Created by nik on 1/2/18.
  */
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
import org.apache.spark.sql.functions.udf
/**
  * Created by nik on 1/2/18.
  */
object KafkaSparkSql {
  val topics = "twitterstream"    // command separated list of topics
  val brokers = "localhost:29092"   // comma separated list of broker:host
  val windowSize = Duration(30000L)          // 10 seconds
  val slidingInterval = Duration(10000L)      // 2 seconds
  val checkpointInterval = Duration(20000L)  // 20 seconds
  val batchIntervalSeconds = 2
  val checkpointDir = "file:///tmp"
  val sentiment = (s:String) => {
    "positive"
  }
  def creatingFunc(): StreamingContext = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaSparkWindows")

    // Create a StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(batchIntervalSeconds))

    val sqlContext = SparkSession
      .builder()
      .getOrCreate().sqlContext
    sqlContext.udf.register("sentiment", sentiment)
    val wordStream = createKafkaStream(ssc).foreachRDD(
      rdd => {
        val row = sqlContext.read.json(rdd.toString).toDF()
          row.show
        rdd.take(1)

        //.withColumn("sentiment", sentiment(co)).createOrReplaceTempView("tweet")
      }
    )

    ssc.remember(Minutes(1))

    ssc.checkpoint(checkpointDir)

    println("Creating function called to create new StreamingContext")
    ssc
  }
  def createKafkaStream(ssc: StreamingContext) = { //: DStream[(String, String)] = {
    val topicsSet = topics.split(",").toSet
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
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
  def main(args: Array[String]) {

    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    ssc.start()

    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)
  }
}

