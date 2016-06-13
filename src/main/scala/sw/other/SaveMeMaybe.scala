package sw.other

import org.apache.spark._

object Names extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("spark://garden.local:7077")
    .set("spark.eventLog.enabled", "true")
  val sc = new SparkContext(sparkConf)
  sc.setCheckpointDir("chpt")

  sc.addJar("target/scala-2.10/fat.jar")

  val partitioner = new HashPartitioner(16)

  val wc = sc.textFile("src/main/resources/all-shakespeare.txt")
    .map(_.toLowerCase())
    .flatMap(_.split("""\W+"""))
    .map(w => (w, 1))
    .reduceByKey(_ + _)

  wc.setName("wc-rdd")

  wc.checkpoint()

  def names(path: String) = sc
    .textFile(path)
    .map(_.toLowerCase())
    .map(_.split("""\W+"""))
    .map(arr => (arr(0), arr(5)))

  val maleNames = names("src/main/resources/male-names.txt")
  maleNames.setName("maleNames-rdd")
  val maleFreq = maleNames.join(wc)

  val femaleNames = names("src/main/resources/female-names.txt")
  femaleNames.setName("femaleNames-rdd")
  val femaleFreq = femaleNames.join(wc)

  val all = femaleFreq ++ maleFreq
  all.setName("all-rdd")

  all.collect().foreach(println)

  sc.stop()
}
