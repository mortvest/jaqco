import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

object Main {
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    // val sql = args(0)
    val sql = "SELECT some_attribute FROM some_relation WHERE 1 = c"
    val query = parser.createStatement(sql)
    query match {
      case q: Query  => println(RelAlg(q)); println(Physical(RelAlg(q)))
      case q: Delete => println("This is a delete!")
      case q: Insert => println("This is an insert!")
      case _ => println("Operation is not supported")
    }
  }
}
