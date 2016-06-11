package sw.pairrdds

import org.apache.spark.{SparkConf, SparkContext}

object ThisWillNotCompile extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")

  val weird = allShakespeare
    // .reduceByKey {
      // case (acc, length) => acc + length
    // }

  weird.take(5).foreach(println)

  sc.stop()
}
