import java.io.IOException
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.Date
import scala.util.Random

object WebPageLogsGenerator extends App {

  val ids = (1 to 10).toList
  val pages = List("/", "/login", "/profile", "/dashboard", "/profile/edit")

  val listener = new ServerSocket(9999)
  var socket: Socket = null
  try {
    socket = listener.accept();
    while (true) {
      val out: PrintWriter = new PrintWriter(socket.getOutputStream(), true);
      out.println(generateLog())
      Thread.sleep(args(0).toInt)

    }
  } finally {
    socket.close();
    listener.close();
  }

  def generateLog(): String = {
    val date  = new Date().toString
    val id = ids((System.nanoTime % ids.length).toInt).toString
    val page = pages((System.nanoTime % pages.length).toInt).toString
    s"$date - $id - $page"
  }
}
