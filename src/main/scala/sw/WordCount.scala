package sw

import org.apache.spark._

object WordCount1 extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val wc = sc.textFile("src/main/resources/all-shakespeare.txt")
    .flatMap(_.split("""\W+"""))
    .groupBy(identity)
    .mapValues(w => w.size)
    .sortBy(_._2, ascending = false)
    .take(10)

  wc.foreach(println)

  sc.stop()

}
