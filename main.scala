import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

// case class TableMetaData(relName: String, indexName: String, tables: Map[String, String])
object Main {
  // TODO: uppercase/lowercase inside the metadata
  val meta = Map[String, TableMetaData]("account_relation" ->
    TableMetaData("account_relation", List("account_id"), Map("account_id" -> "int", "balance" -> "float")))
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    // val sql = args(0)
    val sql = "SELECT account_id, balance FROM ACCOUNT_RELATION WHERE account_id = account_name"
    // val sql = "SELECT account_id, balance FROM ACCOUNT_RELATION WHERE account_id = account_name AND (account_id = 13 OR balance < 12)"
    val query = parser.createStatement(sql)
    query match {
      case q: Query  =>
        println("\n*** QUERY GIVEN:\n")
        println(sql)
        println("\n*** RELATIONAL ALGEBRA:\n")
        println(RelAlg(q))
        println("\n*** PHYSICAL PLAN:\n")
        println(Physical(RelAlg(q), meta))
        println("\n*** GENERATED CODE:\n")
        println(CodeGeneration(Physical(RelAlg(q), meta), "customer_id_key"))
      // TODO INSERT
      // case q: Insert => println("This is an insert!")
      // TODO CREATE
      // case q: Create => println("This is a create")
      // TODO Updaate - also in parser...
      // case q: Update => println("This is a create")
      case _ => println("Operation is not supported")
    }
  }
}
