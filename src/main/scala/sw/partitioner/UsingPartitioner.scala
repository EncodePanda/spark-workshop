package sw.partitioner

import org.apache.spark._

object Partitioner1 extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val partitioner = new HashPartitioner(16)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")
    .map(line => (line, line.length()))

  allShakespeare.take(5).foreach(println)

  sc.stop()
}

object Partitioner2 extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val partitioner = new HashPartitioner(16)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")

  val startings = allShakespeare
    .filter(_.trim != "")
    .groupBy(_.charAt(0))
    .mapValues(_.size)
    .reduceByKey(_ + _)

  startings.take(5).foreach(println)

  sc.stop()
}

object Join extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("spark://localhost:7077")
    .set("spark.eventLog.enabled", "true")
  val sc = new SparkContext(sparkConf)
  sc.addJar("target/scala-2.10/fat.jar")

  def wc = sc.textFile("src/main/resources/all-shakespeare.txt")
    .flatMap(_.split("""\W+"""))
    .map(_.toLowerCase())
    .map(w => (w, 1))
    .reduceByKey(_ + _)
    .map(identity)

  def heros = sc.textFile("src/main/resources/all-shakespeare.txt")
    .flatMap(_.split("""\W+"""))
    .map(_.toLowerCase())
    .filter(word => word == "juliet" || word == "romeo")
    .map {
      case "juliet" => ("juliet", 100)
      case "romeo" => ("romeo", 200)
    }

  val join = wc.join(heros)

  join.collect().foreach(println)

  sc.stop()

}

