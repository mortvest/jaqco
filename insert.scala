import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._
import LogicalPlanGenerator._
import CodeGenUtils._
import PhysicalPlanGenerator._
import Utils._

case class Insertion(map: Map[String, Expr], meta: TableMetaData)

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
          val filledExpr = findForeignTypes(x._2, metaMap, colType)
          val retVal = (x._1, filledExpr)
          val exprType = typeLookup(filledExpr, metaMap.map {
          // val exprType = typeLookup(x._2, metaMap.map {
            case x => (x._1 -> RelationMetaData(x._2.attributes, "", ""))
            }
          )
          // println(s"Type Wanted: $colType, got: $exprType")
          (colType, exprType) match {
          case (SimpleType(colType), SimpleType(valType)) if colType == valType => retVal
          case (StringType(colSize), StringType(valSize)) =>
              // println(s"Size Wanted: $colSize, got: $valSize")
              if (colSize >= valSize) retVal
              else throw new Error(s"Value is too long for type CHAR(${colSize})")
          case _ => throw new Error(s"""Value type does not match type of column "${x._1}"""")
        }
      }
    }
    val insertOp = Insertion(zipped.toMap, meta)
    println(zipped)
    taggify(codeGen(insertOp, queryNum), query)
  }

  // def condTrans(expr: Expr, meta: Map[String, RelationMetaData], postFix: String): String = {
  // def typeLookup(expr: Expr, metaMap: Map[String, RelationMetaData]): DataType = {
  // def genAssign(ty: DataType, name: String, value: String, new_struct: String) = {
  // def codeGen(map: Map[String, Expr], meta: TableMetaData, qNum: Int) = {
  def codeGen(op: Insertion, qNum: Int) = {
    def copyVal(columnName: String, expr: Expr, structName: String): String = {
      val dummyMap = Map("" -> RelationMetaData(op.meta.attributes, "",""))
      val valType = typeLookup(expr, dummyMap)
      val value = condTrans(expr, dummyMap, "")
      genAssign(valType, columnName, value, structName)
    }
    val checkExpr = (ex: Expr) =>
    if (isConstExpr(ex, op.meta))
      ex
    else
      throw new Error(s"Invalid expresion inside INSERT statement")
    val keyMap = op.map.filter { case x => op.meta.indexParts.contains(x._1) }
    val queryTag = s"query${qNum}"
    val indexName = s"${queryTag}_index"
    val keyName = s"${queryTag}_key"
    val keyType = s"${op.meta.relName}_key_type"
    val valName = s"${queryTag}_val"
    val valType = s"${op.meta.relName}_val_type"
    val copyFun = (stName: String, map: Map[String, Expr]) =>
    map.foldLeft("")((acc, x) => acc + copyVal(x._1, x._2, stName) + "\n")
    s"""auto ${indexName} = get_index("${op.meta.relName}");
${keyType} ${keyName} {};
${copyFun(keyName, keyMap)}
${valType} ${valName} {};
${copyFun(valName, op.map)}
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
