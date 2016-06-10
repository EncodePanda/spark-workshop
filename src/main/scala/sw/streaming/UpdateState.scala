package sw.streaming

import java.util.Date
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

case class UserState(visited: List[String]) {
  override def toString = s"visited: $visited"
}

object UpdateState extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  val interval = 10
  val ssc = new StreamingContext(sc, Seconds(interval))
  ssc.checkpoint("checkpoint")

  val logs = ssc.socketTextStream("localhost", 9999)

  val pagesPerUser = logs.map {
    case line =>
      val date :: id :: page :: Nil = line.split(" - ").toList
      (id, page)
  }

  val update = (pages: Seq[String], maybeState: Option[UserState]) =>  maybeState match {
    case None => Some(UserState(pages.toList))
    case Some(state) => Some(state.copy(visited = pages.toList ++ state.visited))
  }

  val sessions = pagesPerUser.updateStateByKey(update)

  sessions.print()

  ssc.start()

  ssc.awaitTermination()
}
