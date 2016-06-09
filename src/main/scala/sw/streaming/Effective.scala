package sw.streaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object Effective extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val interval = 10
  val ssc = new StreamingContext(sc, Seconds(interval))
  ssc.checkpoint("checkpoint")

  val lines = ssc.socketTextStream("localhost", 9999)

  lines.flatMap(_.split("""\W+"""))
    .map(w => (w, 1))
    .reduceByKeyAndWindow(_ + _, _ - _, Seconds(interval * 3)
)
    .print()

  ssc.start()

  ssc.awaitTermination()
}
