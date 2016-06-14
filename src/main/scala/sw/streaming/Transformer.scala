package sw.streaming

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object BatchProcessing {

  def wc(rdd: RDD[String]) =
    rdd.map(_.toLowerCase())
      .flatMap(_.split("""\W+"""))
      .map(w => (w, 1))
      .reduceByKey(_ + _)

}

object Transformer extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sc, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)

  val wc = lines.transform { rdd =>
    BatchProcessing.wc(rdd)
  }

  wc.print()

  ssc.start()

  ssc.awaitTermination()
}
