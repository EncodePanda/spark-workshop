package sw.rdds

import org.apache.spark._

object OnlyJuliet extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val all = sc.textFile("src/main/resources/all-shakespeare.txt")
  val onlyJuliet = all.filter(_.contains("JULIET"))

  onlyJuliet.collect.foreach(println)

  sc.stop()

}

