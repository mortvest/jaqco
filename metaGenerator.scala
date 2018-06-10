import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

object MetaGenerator {
  def translateType(sqlType: String) = {
    val stringPattern = "CHAR\\(([1-9][0-9]*)\\)".r
    sqlType match {
      case "INT" => SimpleType("std::int32_t")
      case "LONG" => SimpleType("std::int64_t")
      case "BOOL" => SimpleType("bool")
      case stringPattern(size) => StringType(size.toInt)
      case _ => throw new Error(s"Type ${sqlType} is not supported (yet)")
    }
  }
  def genAttributes(lst: List[TableElement]): (List[(String, DataType)]) = {
    lst match {
      case Nil => Nil
      case (x:ColumnDefinition)::xs =>
        val name = x.getName.getValue()
        val valType = x.getType.toString
        (name -> translateType(valType)) :: genAttributes(xs)
      case _ => throw new Error(s"DDL meta creation failed")
    }
  }
  def findAttName(keyName: String, map: Map[String, DataType]) = {
    map get keyName match {
      case None => throw new Error(s"""Key part "${keyName}" is not an attribute""")
      case Some(attName) => attName
    }
  }
  def apply(stmt: CreateTable): (String, TableMetaData) = {
    val name = stmt.getName.toString
    val attributes = genAttributes(stmt.getElements.asScala.toList).toMap
    val fullPattern = " *KEY *= *(.+)".r
    val singlePattern = "([A-Za-z_-]+)".r
    val keyParts = stmt.getComment.asScala match {
      case None => Nil
      case Some(content) =>
        content match {
        case fullPattern(part) =>
            singlePattern.findAllIn(part).toList.map{ case x =>
              (x -> findAttName(x, attributes)) }.toList
        // case _ => throw new Error(s"Key pattern ${content} is unrecognizable")
        case _ => Nil
      }
    }
    keyParts match {
      // case Nil => throw new Error(s"No key given in the table: $name")
      case Nil => (name, TableMetaData(name, attributes, attributes))
      case x => (name, TableMetaData(name, x.toMap, attributes))
    }
  }
}
