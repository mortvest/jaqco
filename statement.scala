import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

object StatementProcessor {
  // TODO: uppercase/lowercase inside the metadata
  def apply(query: String, queryNum: Int, retName: String, meta: Map[String, TableMetaData]) = {
    val parser = new SqlParser()
    parser.createStatement(query) match {
      case q: Query => retName match {
        case "" => throw new Error(s"SELECT statement needs an output variable")
        case retName =>
          // println(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta))
          CodeGeneration(
            PhysicalPlanGenerator(
              LogicalPlanGenerator(q),
              meta),
            retName,
            queryNum,
            query)
      }
      case q: Insert => InsertStatement(q, meta, queryNum, query)
      case _ => throw new Error(s"Statement is not supported")
    }
  }
}
