import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._
import LogicalPlanGenerator._
import CodeGenUtils._
import PhysicalPlanGenerator._
import Utils._

case class Insertion(tableName: String, map: Map[String, Const])

object InsertStatement {
  def apply(dmlTree: Insert, metaMap: Map[String, TableMetaData], queryNum: Int, query: String): String = {
    val tableName = dmlTree.getTarget().toString
    val columns = dmlTree.getColumns.asScala match {
      case None => throw new Error("Column list is required in the INSERT statement")
      case Some(lst) => lst.asScala.toList.map { case x => x.toString }
    }
    checkDuplicates(columns, "Column")
    val valueList =
       (dmlTree)
        .getQuery
        .getQueryBody
        .asInstanceOf[Values]
        .getRows
        .asScala
        .toList
        .head
        .asInstanceOf[Row]
        .getItems
        .asScala
        .toList
    val exprList = valueList.map {case x => parseExp(x)}
    val meta = getMeta(metaMap, tableName)
    checkColumns(columns, meta)
    checkLists(exprList, columns)
    val zipped = (columns zip exprList).map {
      case x => meta.attributes get x._1 match {
        case None => throw new Error(s"Column ${x._1} does not exist")
        case Some(colType) =>
          val exprType = typeLookup(x._2, metaMap.map {
            case x => (x._1 -> RelationMetaData(x._2.attributes, "", ""))
            }
          )
          (colType, exprType) match {
          case (SimpleType(colType), SimpleType(valType)) if colType == valType => x
          case (StringType(colSize), StringType(valSize)) =>
            if (colSize > valSize) x
            else throw new Error(s"Value is too long for type CHAR(${colSize})")
          case _ => throw new Error(s"""Value type does not match type of column "${x._1}"""")
        }
      }
    }
    taggify(codeGen(zipped.toMap, meta, queryNum), query)
  }
  def codeGen(map: Map[String, Expr], meta: TableMetaData, qNum: Int) = {
    println(meta.indexParts)
    def keyInit(lst: List[String]): String = {
      lst match {
        case x::xs =>
          condTrans((map get x).get, Map("" -> RelationMetaData(Map(), "","")), "") + ", " + keyInit(xs)
        case Nil => ""
      }
    }
    val queryTag = s"query${qNum}"
    val indexName = s"${queryTag}_index"
    val keyName = s"${queryTag}_key"
    val keyType = s"${meta.relName}_key_type"
    val valName = s"${queryTag}_val"
    val valType = s"${meta.relName}_val_type"
    val valInit = map.foldLeft("") ( (acc, x) =>
      acc + condTrans(x._2, Map("" -> RelationMetaData(Map(), "","")), "") + ", ")
    s"""auto ${indexName} = get_index("${meta.relName}");
${keyType} ${keyName} {${keyInit(meta.indexParts.map{ case x => x._1 }.toList).dropRight(2)}};
${valType} ${valName} {${valInit.dropRight(2)}};
${indexName}->insert(utility::encode_safe(${keyName}),
utility::encode_safe(${valName}));"""
  }

  def checkColumns(columnList: List[String], meta: TableMetaData) = {
    val missing = meta.attributes.filter { case x => !columnList.contains(x._1) }
    if (missing.size > 0) throw new Error(s"""Column \"${missing.head._1}\" is missing """)
  }
  def checkLists(exprList: List[Expr], columnList: List[String]): Unit = {
    (exprList, columnList) match {
      case (Nil, x::_) => throw new Error(s"""Column \"${x}\" needs a value""")
      case (x::xs, y::ys) => checkLists(xs, ys)
      case _ => {}
    }
  }
}
