import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

object MetaGenerator {
  def translateType(sqlType: String) = {
    val stringPattern = "CHAR\\(([1-9][0-9]*)\\)".r
    sqlType match {
      case "INT" => SimpleType("int")
      case "LONG" => SimpleType("long")
      case "BOOL" => SimpleType("bool")
      case stringPattern(size) => StringType(size.toInt)
      case _ => throw new Error(s"Type ${sqlType} is not supported (yet)")
    }
  }
  def genAttributes(lst: List[TableElement]): (Map[String, DataType]) = {
    lst match {
      case Nil => Map[String, DataType]()
      case (x:ColumnDefinition)::xs =>
        val name = x.getName.getValue()
        val valType = x.getType.toString
        genAttributes(xs) + (name -> translateType(valType))
      case _ => throw new Error(s"DDL meta creation failed")
    }
  }
  def findIndexParts(lst: List[TableElement]): List[String] = {
    lst match {
      case Nil => Nil
      case (x:ColumnDefinition)::xs =>
        val name = x.getName.getValue
        toScala(x.getComment) match {
          case None => findIndexParts(xs)
          case Some(comment) =>
            comment.capitalize match {
              case "KEY" => name :: findIndexParts(xs)
              case _ => findIndexParts(xs)
            }
        }
      case _ => throw new Error(s"DDL meta creation failed")
    }
  }
  def apply(stmt: CreateTable): (String, TableMetaData) = {
    val name = stmt.getName.toString
    val elements = stmt.getElements.asScala.toList
    findIndexParts(elements) match {
      case Nil => throw new Error(s"No key given in the table: $name")
      case x => (name, TableMetaData(name, x, genAttributes(elements)))
    }
  }
}
