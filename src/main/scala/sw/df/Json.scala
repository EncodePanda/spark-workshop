package sw.df

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import sw.air._

object JsonSchemaAndShow extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val enron = sqlCtx.read.json("src/main/resources/enron/enron.json")
  enron.printSchema
  // enron.select($"date", $"sender", $"subject").filter($"subject" contains("year")).show()

  sc.stop()

}

object MoreStruct extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val enron = sqlCtx.read.json("src/main/resources/enron/enron.json")
  enron.select($"date", $"sender", $"subject", explode($"to")).filter($"subject" contains("year")).show()

  sc.stop()
}

object ToJson extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)
  import sqlCtx._
  import sqlCtx.implicits._

  val enron = sqlCtx.read.json("src/main/resources/enron/enron.json")
  val reunion = enron.select($"date", $"sender", $"subject", explode($"to")).filter($"subject" contains("year"))
  reunion.toJSON.collect().foreach(println)

  sc.stop()
}
