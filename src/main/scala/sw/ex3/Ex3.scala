package sw.ex2

import org.apache.spark._

object Startings extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")

  val startings = allShakespeare
    .filter(_.trim != "")
    .map(line => (line.charAt(0), line))
    .mapValues(_.size)
    .reduceByKey {
      case (acc, length) => acc + length
    }
    .sortByKey()

  startings.take(5).foreach(println)

  sc.stop()
}

