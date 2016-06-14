package sw.other

import org.apache.spark._

object DontLooseMe extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")

  val sc = new SparkContext(sparkConf)

  val data1 = List((1, "a"), (2,  "b"), (3, "c"))
  val data2 = List((0, "x"), (2,  "y"), (4, "z"))

  val rdd1 = sc.parallelize(data1)
  val rdd2 = sc.parallelize(data2)

  val iMKinddaLost = rdd1.join(rdd2).collect().foreach(println)
  
  sc.stop()
}
