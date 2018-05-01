import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

// case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, String])

object MetaGenerator {
  def translateType(sqlType: String) = {
    sqlType match {
      case "INT" => "int"
      case "LONG" => "long"
      case "VARCHAR" => "std::string"
      case _ => throw new Error(s"Type ${sqlType} is not supported")
    }
  }
  def genAttributes(lst: List[TableElement]): (Map[String, String]) = {
    lst match {
      case Nil => Map[String, String]()
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
            comment match {
              case "KEY" | "key" => name :: findIndexParts(xs)
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
