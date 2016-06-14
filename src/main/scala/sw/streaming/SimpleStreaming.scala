package sw.streaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object SimpleStreaming extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)

  lines.print()

  ssc.start()

  ssc.awaitTermination()
}

object SimpleStreaming2 extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)

  lines.count().print()

  ssc.start()

  ssc.awaitTermination()
}

object SimpleStreaming3 extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)

  val upperW = lines.map(_.toUpperCase()).filter(_.startsWith("W"))
  upperW.print()

  ssc.start()

  ssc.awaitTermination()
}


