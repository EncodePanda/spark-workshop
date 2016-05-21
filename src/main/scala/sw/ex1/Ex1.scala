package sw.ex1

import org.apache.spark._

object FullScan extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val allShakespeare = sc.textFile("src/main/resources/all-shakespeare.txt")

  println("NUMBER OF LINES")
  println(allShakespeare.count)

  println("Collecting all elements, printing first 5")
  allShakespeare.collect().take(5).foreach(println)

  println("Taking sample of 5 elements")
  allShakespeare.take(5).foreach(println)
  
  sc.stop()

}

object OnlyJuliet extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val allWithJuliet = sc.textFile("src/main/resources/all-shakespeare.txt")
    .filter(_.toLowerCase().contains("juliet"))

  allWithJuliet.collect.foreach(println)

  sc.stop()

}

