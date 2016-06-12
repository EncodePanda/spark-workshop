package sw.utils

import java.io._
import scala.io.Source

object FileUtil {

  def delete(f: File): Unit = {
    if (f.isDirectory()) {
      f.listFiles().foreach {
        sf => delete(sf)
      }
    }
    if (!f.delete())
      throw new FileNotFoundException("Failed to delete file: " + f);
  }
}
