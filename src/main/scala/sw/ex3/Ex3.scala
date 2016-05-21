package sw.ex2

import org.apache.spark._

object Temp extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")
  

  sc.stop()
}

