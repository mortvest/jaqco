import sys.process._
import scala.io.Source
import java.io._

case class QueryObject (lineStart: Int, lineStop: Int, content: String)

object Preprocessor {
  def processDDL(filename: String) = {
    val file = Source.fromFile(filename).getLines.mkString
    val query = raw"<q\*>(.*?)<\*q>".r
    query.findAllIn(file).map{case query(x) => x}.toList
  }
}
