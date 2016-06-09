package sw.streaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object Windows extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val interval = 10
  val ssc = new StreamingContext(sc, Seconds(interval))
  ssc.checkpoint("checkpoint")

  val lines = ssc.socketTextStream("localhost", 9999)
  val windowed = lines.window(Seconds(interval * 3), Seconds(interval * 2))

  windowed.flatMap(_.split("""\W+"""))
    .map(w => (w, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()

  ssc.awaitTermination()
}
