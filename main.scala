import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

// case class TableMetaData(relName: String, indexName: String, tableNames: List[String])
object Main {
  val meta = Map[String, TableMetaData]("accounts" -> TableMetaData("accounts", "a", List("a","b")))
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    // val sql = args(0)
    val sql = "SELECT price, 2 FROM accounts WHERE price < 1 and money = 2"
    val query = parser.createStatement(sql)
    query match {
      case q: Query  =>
        println(RelAlg(q))
        println(Physical(RelAlg(q), meta))
        println(CodeGeneration(Physical(RelAlg(q), meta)))
      case q: Delete => println("This is a delete!")
      case q: Insert => println("This is an insert!")
      case _ => println("Operation is not supported")
    }
  }
}
