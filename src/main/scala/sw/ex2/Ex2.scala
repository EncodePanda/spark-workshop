package sw.ex2

import org.apache.spark._

object SomeActions extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val evenNumbers = sc.parallelize(1 to 1000).filter(_ % 2 == 0)

  println("sum: " + evenNumbers.reduce(_ + _))
  println("first even: " + evenNumbers.first)
  println("Printing all even, that can be divided by 150")
  println(evenNumbers.filter(_ % 150 == 0).foreach(n => println(s"Hey I'm $n and I can be divided by 150!'")))

  sc.stop()
}



