import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import org.rogach.scallop._
import scala.io.Source

// // Setting up command line arguments
// class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
//   // val debug = opt[Boolean](descr = "Debug mode")
//   val ddlFile = opt[String](descr = "DDL file", required = true)
//   val files = trailArg[List[String]](required = true)
//   verify()
// }
// object Main{
//   def checkExt(fileName: String, ext: String) = {
//     val pattern = s".+.${ext}".r
//     pattern.findFirstMatchIn(fileName) match {
//       case None => throw new Error(s"File ${fileName} must have extension .${ext}")
//       case Some(_) => {}
//     }
//   }
//   def createMeta(sql: String) = {
//     val parser = new SqlParser()
//     val query = parser.createStatement(sql)
//     MetaGenerator(query.asInstanceOf[CreateTable])
//   }

//   def main(args: Array[String]) = {
//     val conf = new Conf(args)
//     val files = conf.files()
//     val ddlFile = conf.ddlFile()
//     checkExt(ddlFile, ".ddl")
//     for (fileName <- files) {checkExt(fileName, ".jpp")}
//     val ddlList = Preprocessor.processDDL(ddlFile)
//     val st = ddlList.map{ case x => createMeta(x) }
//     st.groupBy(_._1).collect { case (x,ys) if ys.lengthCompare(1) > 0 => x } match {
//       case Nil => {}
//       case x::xs => throw new Error(s"Table ${x} is created twice")
//     }
//     val meta = st.toMap
//     val ddlCode = DDLProcessor(meta)
//     val fileCode = files.map {case c => Preprocessor.processFile(c)}
//     println(fileCode)
//   }
// }

// case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, DataType])

object Main {
  // TODO: uppercase/lowercase inside the metadata
  val meta = Map[String, TableMetaData]("test_relation" ->
    TableMetaData("test_relation", List("account_name"), Map("account_name" -> StringType(255), "balance" -> SimpleType("int"), "account_id" -> SimpleType("int"))))
  def main(args: Array[String]) = {
    val parser = new SqlParser()
    val sql = "SELECT account_name, account_name, 'sup' FROM test_relation WHERE account_name <= 'hello' AND account_name >= 'zzzz'"
    // val sql = "SELECT account_name, account_name, 'sup' FROM test_relation WHERE account_id <= 12"
    // val sql = "CREATE TABLE some_table(val INT, id INT COMMENT 'KEY', name VARCHAR(19))"
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
        println(CodeGeneration(Physical(RelAlg(q), meta), "customer_id_key", 0))
      // TODO INSERT
      // case q: Insert => println("This is an insert!")
      case q: CreateTable =>
        val meta = MetaGenerator(q)
        println(meta)
        // println(DDLProcessor(meta))
      // TODO Updaate - also in parser...
      // case q: Update => println("This is a create")
      case _ => println("Operation is not supported")
    }
  }
}
