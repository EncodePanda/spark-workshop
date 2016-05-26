package sw.ex7

import org.apache.spark._

object Names extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("spark://localhost:7077")
    .set("spark.eventLog.enabled", "true")
  val sc = new SparkContext(sparkConf)

  sc.addJar("target/scala-2.10/fat.jar")

  val partitioner = new HashPartitioner(16)

  val wc = sc.textFile("src/main/resources/all-shakespeare.txt")
    .map(_.toLowerCase())
    .flatMap(_.split("""\W+"""))
    .map(w => (w, 1))
    .reduceByKey(_ + _)
    .cache()

  def names(path: String) = sc
    .textFile(path)
    .map(_.toLowerCase())
    .map(_.split("""\W+"""))
    .map(arr => (arr(0), arr(5)))

  val maleNames = names("src/main/resources/male-names.txt")
  val maleFreq = maleNames.join(wc)
  maleFreq.collect().foreach(println)


  val femaleNames = names("src/main/resources/female-names.txt")
  val femaleFreq = femaleNames.join(wc)
  femaleFreq.collect().foreach(println)


  sc.stop()
}
