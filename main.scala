import org.rogach.scallop._
import better.files._
import better.files.Dsl._
import File._
import java.io.{File => JFile}
import scala.io.Source
import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

import Preprocessor._
import Utils._
case class SimpleRef(counter: Int, fun: (Int => Unit))

// Setting up command line arguments
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val codeFiles = opt[List[String]](descr = "List of code files with embedded SQL(.jsql)")
  val ddlFiles = opt[List[String]](descr = "List of DDL files(.ddl)")
  val outDir = opt[String](descr = "Destination folder", short = 'o')
  val folder = opt[String](descr = "Folder with .ddl and .jsql files")
  val debug = toggle("debug", short = 'g')
  requireOne(codeFiles, folder)
  codependent(codeFiles, ddlFiles)
  verify()
}

object Main{
  val codeExt = ".jsql"
  val ddlExt = ".ddl"
  val filePostfix = "jaqco"
  val defaultFolder = "jaqco_generated"

  def main(args: Array[String]) = {
    val conf = new Conf(args)
    val outDir = (conf.outDir.toOption, conf.folder.toOption) match {
      case (Some(dir), _) => dir
      case (None, Some(dir)) => s"$dir/$defaultFolder"
      case _ => ""
    }
    (conf.codeFiles.toOption, conf.ddlFiles.toOption, conf.folder.toOption) match {
      case (Some(codeFiles), Some(ddlFiles), None) =>
        checkDuplicates(ddlFiles, "File")
        checkDuplicates(codeFiles, "File")
        for (fileName <- codeFiles) {checkExt(fileName, codeExt)}
        for (fileName <- ddlFiles) {checkExt(fileName, ddlExt)}
        processFiles(ddlFiles, codeFiles, outDir)
      case (None, None, Some(folder)) =>
        // println(s"folder is: ${folder}")
        val dirList = ls(File(folder)).toList.map {case x => x.toString}
        val codeFiles = extFilter(dirList, codeExt)
        if (codeFiles.size < 1)
          throw new Error(s"""No code files(.jsql) were found in the folder "${folder}"""")
        val ddlFiles = extFilter(dirList, ddlExt)
        if (codeFiles.size < 1)
          throw new Error(s"""No DDL files(.ddl) were found in the folder "${folder}"""")
        processFiles(ddlFiles, codeFiles, outDir)
      case _ => throw new Error(s"Command line argument processing failed")
    }
  }
  def processFiles(ddlFiles: List[String], codeFiles: List[String], outDir: String) = {
    // println(s"Code files: ${codeFiles}")
    // println(s"DDL files: ${ddlFiles}")
    val ddlCode = ddlFiles.map { case x => File(x).contentAsString }
    val ddlList =
      ddlCode.foldLeft(List[String]()) ((acc, x) => Preprocessor.processDDL(x) ::: acc)
    val st = ddlList.map{ case x => createMeta(x) }
    st.groupBy(_._1).collect { case (x,ys) if ys.lengthCompare(1) > 0 => x } match {
      case Nil => {}
      case x::xs => throw new Error(s"Table ${x} is created twice")
    }
    val meta = st.toMap
    val (scheCrea, typeDef) =
      DDLProcessor(meta, s"schema_creator_$filePostfix.h", s"type_definition_$filePostfix.h")
    processCodeFiles(codeFiles, meta)
  }
  def processCodeFiles(fileList: List[String], meta: Map[String, TableMetaData]) = {
    var queryCounter = 0
    def processCodeFile(fileName: String, meta: Map[String, TableMetaData], ref: SimpleRef) = {
      val ref = SimpleRef(queryCounter, (queryCounter = _))
      val fileCode = File(fileName).contentAsString
      processFile(fileCode, ref, meta)
    }
    fileList.foreach { case x =>
      val ref = SimpleRef(queryCounter, (queryCounter = _))
      println(processCodeFile(x, meta, ref))
    }
  }
  def checkExt(fileName: String, ext: String) = {
    val pattern = s".+${ext}".r
    pattern.findFirstMatchIn(fileName) match {
      case None => throw new Error(s"File ${fileName} must have extension .${ext}")
      case Some(_) => {}
    }
  }
  def extFilter(dirList: List[String], ext: String) = {
    dirList.filter{ case x => (s".+${ext}"r).findFirstIn(x).isDefined }
  }
  def createMeta(sql: String) = {
    val parser = new SqlParser()
    val query = parser.createStatement(sql)
    MetaGenerator(query.asInstanceOf[CreateTable])
  }
  def createFile(dirName: String, fileName: String, content: String) = {
    (s"$dirName/$fileName")
      .toFile
      .createIfNotExists()
      .overwrite(content)
  }
}
//   def main(args: Array[String]) = {
//     val retName = "customer_id_key"
//     val queryNum = 0
//     val sql = "SELECT account_id FROM test_relation WHERE account_id <= LONG_VAR_buyer_name"
//     // val meta = Map[String, TableMetaData](
//     //   "test_relation" ->
//     //     TableMetaData("test_relation", List("user_name"),
//     //       Map("user_name" -> StringType(255),
//     //         "account_id" -> SimpleType("long"),
//     //         "balance" -> SimpleType("long")
//     //       ))
//     // )
//     val meta = Map[String, TableMetaData](
//       "test_relation" ->
//         TableMetaData("test_relation", List("account_id"),
//           Map("account_id" -> SimpleType("long"),
//             "user_name" -> StringType(255),
//             "balance" -> SimpleType("long")
//           ))
//     )
//     val parser = new SqlParser()
//     parser.createStatement(sql) match {
//       case q: Query  =>
//         println("\n***QUERY GIVEN:\n")
//         println(sql)
//         println("\n***LOGICAL QUERY PLAN:\n")
//         println(LogicalPlanGenerator(q))
//         println("\n*** PHYSICAL QUERY PLAN:\n")
//         println(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta))
//         println("\n*** GENERATED CODE:\n")
//         println(CodeGeneration(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta),retName, queryNum))
//       // TODO INSERT
//       case q: Insert => println("INSERT STATEMENT")
//       case _ => throw new Error(s"Statement is not supported")
//     }
//   }
// }
