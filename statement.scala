import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

object StatementProcessor {
  // TODO: uppercase/lowercase inside the metadata
  def apply(query: String, queryNum: Int, retName: String, meta: Map[String, TableMetaData]) = {
    val parser = new SqlParser()
    parser.createStatement(query) match {
      case q: Query  =>
        // println("\n***QUERY GIVEN:\n")
        // println(sql)
        // println("\n***LOGICAL QUERY PLAN:\n")
        // println(LogicalPlanGenerator(q))
        // println("\n*** PHYSICAL QUERY PLAN:\n")
        // println(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta))
        // println("\n*** GENERATED CODE:\n")
        // println(CodeGeneration(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta),retName, queryName))
        CodeGeneration(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta), retName, queryNum)
      // TODO INSERT
      case q: Insert => "INSERT STATEMENT"
      case _ => throw new Error(s"Statement is not supported")
    }
  }
}
