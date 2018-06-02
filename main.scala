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
  val ddlFile = opt[String](descr = "DDL file(.ddl), name of the file will be the name of the namespace")
  val outDir = opt[String](descr = "Destination folder", short = 'o')
  val folder = opt[String](descr = "Folder with .ddl and .jsql files", short = 'f')
  val forceClean = toggle("force_clean", descrYes = "Clean the output directory before file generation")
  val debug = toggle("debug", short = 'g')
  requireOne(codeFiles, folder)
  codependent(codeFiles, ddlFile)
  verify()
}

object Main{
  val codeExt = ".jsql"
  val ddlExt = ".ddl"
  val hostLangExt = ".cc"
  val filePostfix = "jaqco"
  val defaultDir = "jaqco_generated"

  def main(args: Array[String]) = {
    val conf = new Conf(args)
    // val debug: Boolean = conf.debug.toOption match {
    //   case None => false
    //   case Some(x) => x
    // }
    // val forceClean = conf.forceClean.toOption match {
    //   case None => false
    //   case Some(x) => x
    // }
    val outDir = (conf.outDir.toOption, conf.folder.toOption) match {
      case (Some(dir), _) => dir
      case (None, Some(dir)) => s"$dir/$defaultDir"
      case _ => s"$defaultDir"
    }
    val forceClean = getToggleOption(conf.forceClean)
    val debug = getToggleOption(conf.debug)
    println(s"Force clean: $forceClean")
    println(s"Debug: $debug")
    (conf.codeFiles.toOption, conf.ddlFile.toOption, conf.folder.toOption) match {
      case (Some(codeFiles), Some(ddlFile), None) =>
        checkDuplicates(codeFiles, "File")
        for (fileName <- codeFiles) {checkExt(fileName, codeExt)}
        checkExt(ddlFile, ddlExt)
        processFiles(ddlFile, codeFiles, outDir, forceClean)
      case (None, None, Some(folder)) =>
        // println(s"folder is: ${folder}")
        val dirList = ls(File(folder)).toList.map {case x => x.toString}
        val codeFiles = extFilter(dirList, codeExt)
        if (codeFiles.size < 1)
          throw new Error(s"""No code files(.jsql) were found in the folder "${folder}"""")
        val ddlFiles = extFilter(dirList, ddlExt)
        if (ddlFiles.size < 1)
          throw new Error(s"""DDL file(.ddl) was not found in the folder "${folder}"""")
        else if (ddlFiles.size > 1)
          throw new Error(s"""Multiple DDL files (.ddl) were not found in the folder "${folder}"""")
        codeFiles.foreach{ case x => println(s"Code file: $x") }
        println(s"DDL file: $ddlFiles.head")
        processFiles(ddlFiles.head, codeFiles, outDir, forceClean)
      case _ => throw new Error(s"Command line argument processing failed")
    }
  }
  def processFiles(ddlFile: String, codeFiles: List[String], outDir: String, force: Boolean) = {
    // println(s"Code files: ${codeFiles}")
    // println(s"DDL file: ${ddlFile}")
    if (force)
      File(outDir).createIfNotExists(true).clear()
    else
      File(outDir).createIfNotExists(true)
    val ddlCode = File(ddlFile).contentAsString
    val ddlList = Preprocessor.processDDL(ddlCode)
    val st = ddlList.map{ case x => createMeta(x) }
    st.groupBy(_._1).collect { case (x,ys) if ys.lengthCompare(1) > 0 => x } match {
      case Nil => {}
      case x::xs => throw new Error(s"Table ${x} is created twice")
    }
    val meta = st.toMap
    val namespace = findFileName(ddlFile, ddlExt)
    val (scheCrea, typeDef) =
      DDLProcessor(meta, s"schema_creator_$filePostfix.h", s"type_definition_$filePostfix.h", namespace)
    println(typeDef)
    createFile(outDir, "jaqco_schema_creator.h", scheCrea)
    createFile(outDir, "jaqco.h", typeDef)
    processCodeFiles(codeFiles, meta, outDir)
  }
  def processCodeFiles(fileList: List[String], meta: Map[String, TableMetaData], outDir: String) = {
    // println(fileList)
    var queryCounter = 0
    def processCodeFile(fileName: String, meta: Map[String, TableMetaData], ref: SimpleRef) = {
      val ref = SimpleRef(queryCounter, (queryCounter = _))
      val fileCode = File(fileName).contentAsString
      processFile(fileCode, ref, meta)
    }
    fileList.foreach { case x =>
      val ref = SimpleRef(queryCounter, (queryCounter = _))
      val fileContents = (processCodeFile(x, meta, ref))
      createFile(outDir, findFileName(x, codeExt) + hostLangExt, fileContents)
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
    val newName = s"$dirName/$fileName"
    newName.toFile.createIfNotExists().overwrite(content)
    println(s"File generated: $newName")
  }
  def findFileName(dest: String, ext: String): String = {
    val pattern = s"(?s).*/(.+)\\${ext}".r
    dest match {
      case pattern(x) => x
      case _ => throw new Error(s"File must have extension ${ext}")
    }
  }
  def getToggleOption(x: ScallopOption[Boolean]) = {
    x.toOption match {
      case None => false
      case Some(x) => x
    }
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
//         TableMetaData("test_relation", Map("account_id" -> SimpleType("long")),
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
//         println(CodeGeneration(PhysicalPlanGenerator(LogicalPlanGenerator(q), meta),retName, queryNum, sql))
//       case _ => throw new Error(s"Statement is not supported")
//     }
//   }
// }
