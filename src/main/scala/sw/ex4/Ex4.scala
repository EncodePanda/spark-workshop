package sw.ex4

import org.apache.spark._

object StagesStagesA extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
    .set("spark.eventLog.enabled", "true")
    .set("spark.executor.memory", "500m")

  val sc = new SparkContext(sparkConf)

  val all = sc.textFile("src/main/resources/all-shakespeare.txt")

  sc.stop()
}

object StagesStagesB extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")

  val sc = new SparkContext(sparkConf)

  val weirdo = sc.textFile("src/main/resources/all-shakespeare.txt")
    .map(_.toUpperCase())
    .groupBy(_.size)
    .mapValues(_.size)

  sc.stop()
}

object StagesStagesC extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
    .set("spark.eventLog.enabled", "true")
    .set("spark.executor.memory", "500m")

  val sc = new SparkContext(sparkConf)

  val priors = sc.parallelize(List(
    ('a', 100),
    ('b', 200)
  ))

  val abs = sc.textFile("src/main/resources/all-shakespeare.txt")
    .filter(line => line.trim != "")
    .map(_.toLowerCase())
    .groupBy(_.charAt(0))
    .filter {
      case (c, lines) => c == 'a' || c == 'b'
    }

  val theFinal = abs.join(priors)

  sc.stop()

}
