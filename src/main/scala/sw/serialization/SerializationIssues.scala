package sw.serialization

import org.apache.spark._

case class User(name: String)

object SerializationIssues extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("spark://garden.local:7077")
  val sc = new SparkContext(sparkConf)

  val five = sc
    .parallelize(List("john", "mike", "kate", "anna"))
    .take(5)
    .foreach(println)

  sc.stop()
}

class Simple {

  def iTakeLambda(lambda: Int => String): String = {
    // val lambda: Int => String = a => a.toString
    lambda(10)
  }

  iTakeLambda(a => a.toString())
}

