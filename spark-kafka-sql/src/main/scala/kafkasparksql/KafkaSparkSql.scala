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
import scala.util.parsing.json._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._/**
  * Created by nik on 1/2/18.
  */
object KafkaSparkSql {
  val topics = "twitterstream"    // command separated list of topics
  val brokers = "localhost:29092"   // comma separated list of broker:host
  val windowSize = Duration(30000L)          // 10 seconds
  val slidingInterval = Duration(10000L)      // 2 seconds
  val checkpointInterval = Duration(20000L)  // 20 seconds
  val batchIntervalSeconds = 2
  val checkpointDir = "file:///tmp/"

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
      .setMaster("local")
      .set("spark.sql.warehouse.dir","file:///tmp")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.sqlContext.implicits._
    // Create a StreamingContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchIntervalSeconds))
//
//    val sqlContext = SparkSession
//      .builder()
//      .getOrCreate().sqlContext
    spark.udf.register("sentiment", sentiment _)
    val wordStream = createKafkaStream(ssc).foreachRDD(
      rdd => {
        rdd.map(row => parser(row._2)).toDF().withColumn("sentiment", callUDF("sentiment", col("value"))).show
        rdd.take(1)

        //.withColumn("sentiment", sentiment(co)).createOrReplaceTempView("tweet")
      }
    )

    ssc.remember(Minutes(1))

    ssc.checkpoint(checkpointDir)

    println("Creating function called to create new StreamingContext")
    ssc
  }

  def sentiment(s:String) : String = {
    val positive = Array("like", "love", "good", "great", "happy", "cool", "the", "one", "that")
    val negative = Array("hate", "bad", "stupid", "is")
    var st = 0;
    val words = s.split(" ")
    positive.foreach(p =>
      words.foreach(w =>
        if(p==w) st = st+1
      )
    )

    negative.foreach(p=>
      words.foreach(w=>
        if(p==w) st = st-1
      )
    )
    if(st>0)
      "positive"
    else if(st<0)
      "negative"
    else
      "neutral"
  }

  def parser(json: String): String = {
    JSON.parseFull(json).get.asInstanceOf[Map[String, Any]].get("text").toString
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
}

