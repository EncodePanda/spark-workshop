package sw.streaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object ReadingFromDirectory extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sc, Seconds(10))
  val logs = ssc.textFileStream("src/main/resources/logs/")

  logs.print()

  ssc.start()

  ssc.awaitTermination()
}
