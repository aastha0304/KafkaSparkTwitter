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
    val sparkConf = new SparkConf().setAppName("KafkaSparkWindows")
    // Create a StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(batchIntervalSeconds))


    val sqlContext = SparkSession
      .builder()
      .getOrCreate().sqlContext
    sqlContext.udf.register("sentiment", sentiment)
    // Get the word stream from the source
    val wordStream = createKafkaStream(ssc).foreachRDD(
      rdd => {
        val row = sqlContext.read.json(rdd.toString).toDF().withColumn("sentiment", sentiment("_1")).createOrReplaceTempView("tweet")


      }
    )
    /**wordStream.foreachRDD( rdd => {
      rdd.saveAsTextFile("file:///tmp")
    })*/
    val runningCountStream = wordStream.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
    /**val runningCountStream = wordStream.reduceByKeyAndWindow(
      (x: Int, y: Int) => x+y,
      //(x: Int, y: Int) => x-y,                // Subtract counts going out of window
      windowSize, slidingInterval, 2,
      (x: (String, Int)) => x._2 != 0)        // Filter all keys with zero counts
      */
    // Checkpoint the dstream so that it can be persisted every 20 seconds. If the stream is not checkpointed, the performance will deteriorate significantly over time and eventually crash.
    runningCountStream.checkpoint(checkpointInterval)

    runningCountStream.print()


    // Create temp table at every batch interval
    runningCountStream.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
      sqlContext.createDataFrame(rdd).toDF("word", "count").registerTempTable("batch_word_count")

      /* Trigger a dummy action to execute the DAG. This triggering of action will ensure that
         the checkpointing is invoked. If there is no action on the DAG, then checkpointing will not
         be invoked and if somebody queries the table after 'n' minutes, it will result in processing
         a big lineage of rdds.
      */
      rdd.take(1)
      rdd.saveAsTextFile("file:///tmp/")
    }

    // To make sure data is not deleted by the time we query it interactively
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
    //KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //ssc, kafkaParams, topicsSet)
  }
  def main(args: Array[String]) {
    /**
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaSparkWindows")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val sqlContext = new SQLContext(sparkConf)
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
     stream.
    // Stop any existing StreamingContext
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
      */
    // Function to create a new StreamingContext and set it up


    // Stop any existing StreamingContext
    val stopActiveContext = true
    if (stopActiveContext) {
      StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
    }

    // Get or create a streaming context.
    val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

    // This starts the streaming context in the background.
    ssc.start()

    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)
  }
}

