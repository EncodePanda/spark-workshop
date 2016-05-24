package sw.ex4

import org.apache.spark._

object WordCount1 extends App {

  val sparkConf = new SparkConf()
    .setAppName("word-count-1")
    .setMaster("spark://localhost:7077")

  val sc = new SparkContext(sparkConf)


  val wc = sc.textFile("src/main/resources/all-shakespeare.txt")
    .flatMap(_.split("""\W+"""))
    .groupBy(identity)
    .mapValues(w => w.size)
    .take(10)
    .foreach(println)

  sc.stop()

}

case class User(name: String)

object SerializationIssues extends App {
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("spark://localhost:7077")
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
