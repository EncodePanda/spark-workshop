package sw.streaming
/*
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
 */
object Kafka extends App {

  /*
    val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val interval = 10
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(interval))
  ssc.checkpoint("checkpoint")


  val topics = Map("test" -> 1)
  val zkQuorum = "localhost:2181"
  val group = "1"
  val topicLines = KafkaUtils.createStream(ssc, zkQuorum, group, topics)
  topicLines.map(_._2).print()

  ssc.start()

  ssc.awaitTermination()
  sc.stop()
   */
}


