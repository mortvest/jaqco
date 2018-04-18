import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

object Main {
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    // val sql = args(0)
    val sql = "SELECT a FROM b WHERE a < 2"
    val query = parser.createStatement(sql)
    query match {
      case q if q.isInstanceOf[Query]  => println(RelAlg(q.asInstanceOf[Query]))
      case q if q.isInstanceOf[Delete] => println("This is a delete!")
      case q if q.isInstanceOf[Insert] => println("This is an insert!")
      case _ => println("Operator is not supported")
    }
  }
}
