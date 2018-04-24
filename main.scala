import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

// case class TableMetaData(relName: String, indexName: String, tables: Map[String, String])
object Main {
  // TODO: uppercase/lowercase inside the metadata
  val meta = Map[String, TableMetaData]("account_relation" ->
    TableMetaData("account_relation", "account_id", Map("account_id" -> "int", "balance" -> "float")))
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    // val sql = args(0)
    val sql = "SELECT account_id, balance FROM ACCOUNT_RELATION WHERE account_id = account_name"
    val query = parser.createStatement(sql)
    query match {
      case q: Query  =>
        println(sql)
        println(RelAlg(q))
        println(Physical(RelAlg(q), meta))
        println(CodeGeneration(Physical(RelAlg(q), meta), "customer_id_key"))
      case q: Delete => println("This is a delete!")
      case q: Insert => println("This is an insert!")
      case _ => println("Operation is not supported")
    }
  }
}
