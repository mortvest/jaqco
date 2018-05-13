final case class Error(private val message: String = "",
  private val cause: Throwable = None.orNull) extends Exception(message, cause)

sealed trait Expr
abstract class BinOp extends Expr {
  def left: Expr
  def right: Expr
}
abstract class RangeOp extends BinOp
case class DerefExp(alias: String, columnName: String) extends Expr
case class Attribute(attName: String) extends Expr
case class OutsideVar(name: String, varType: DataType) extends Expr
case class Less(left: Expr, right: Expr) extends RangeOp
case class Leq(left: Expr, right: Expr) extends RangeOp
case class Greater(left: Expr, right: Expr) extends RangeOp
case class Geq(left: Expr, right: Expr) extends RangeOp
case class Equals(left: Expr, right: Expr) extends BinOp
case class Plus(left: Expr, right: Expr) extends BinOp
case class Minus(left: Expr, right: Expr) extends BinOp
case class And(left: Expr, right: Expr) extends BinOp
case class Or(left: Expr, right: Expr) extends BinOp
case class Not(value: Expr) extends Expr
case class Const(value: String, varType: DataType) extends Expr
// TODO: Add types floats, dates etc

sealed trait DataType
case class SimpleType(typeName: String) extends DataType
case class StringType(size: Int) extends DataType

sealed trait RangeVal
case class ConstVal(expr: Expr) extends RangeVal
case class ZeroVal() extends RangeVal
case class MaxVal() extends RangeVal

case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, DataType])

sealed trait Physical
case class RangeScan(
  meta: TableMetaData,
  from: List[(DataType, RangeVal)],
  to: List[(DataType, RangeVal)],
  alias: String
) extends Physical
case class IndexLookup(meta: TableMetaData, map: Map[String, Expr], alias: String) extends Physical
case class ThetaJoin(tables: List[Physical]) extends Physical

case class JaqcoQuery(source: Physical, selectExpr: Option[Expr], projList: List[Expr])

import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._
import PlanGenerator._

object Physical{
  def findOutsideVar(name: String): Expr = {
    val longPattern   = "LONG_VAR_([A-Za-z0-9]+)".r
    val intPattern    = "INT_VAR_([A-Za-z0-9]+)".r
    val charPattern   = "CHAR_VAR_([A-Za-z0-9]+)".r
    val stringPattern = "VARCHAR([0-9]*)_VAR_([A-Za-z0-9]+)".r
    name match {
      case longPattern(name) => OutsideVar(name, SimpleType("long"))
      case intPattern(name) => OutsideVar(name, SimpleType("int"))
      case charPattern(name) => OutsideVar(name, SimpleType("char"))
      case stringPattern(size, name) => OutsideVar(name, StringType(size.toInt))
      case name => Attribute(name)
    }
  }
  def parseExp(tree: Expression): Expr = {
    tree match {
      case ex: DereferenceExpression =>
        val field = ex.getField.toString
        DerefExp(ex.getBase.toString, field)
      case ex: ComparisonExpression =>
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        val compType = ex.getType.getValue
        compType match {
          case "<" => Less(left, right)
          case "<=" => Leq(left, right)
          case ">" => Greater(left, right)
          case ">=" => Geq(left, right)
          // TODO: moar ops
          case _ => Equals(left, right)
        }
      case ex: ArithmeticBinaryExpression =>
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        val compType = ex.getType.getValue
        compType match {
          case "+" => Plus(left, right)
          // TODO: moar ops
          case _ => Minus(left, right)
        }
      case ex: LogicalBinaryExpression =>
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        val compType = ex.getType.toString
        compType match {
          case "AND" => And(left, right)
          case "OR" => Or(left, right)
        }
      case x: NotExpression => Not(parseExp(x.getValue))
      case x: Identifier => findOutsideVar(x.getValue.toString)
      case x: LongLiteral => Const(x.getValue.toString, SimpleType("long"))
      case x: StringLiteral =>
        val str = x.getValue.toString
        Const(str, StringType(str.size))
    }
  }
  def getTable(rel: AliasedRelation): Table = {
    rel.getRelation match {
      case x: Table => x
      case x => throw new Error(s"Alias on $x is not supported")
    }
  }
  def parseJoin(tree: Join): Map[String, String] = {
    def proc(tree:Join): List[(String, String)] = {
      (tree.getLeft, tree.getRight) match {
        case (left: AliasedRelation, right: AliasedRelation) =>
          val leftRel = getTable(left)
          val rightRel = getTable(right)
          (left.getAlias.toString, leftRel.getName.toString) ::
          (right.getAlias.toString, rightRel.getName.toString) :: Nil
        case (left: AliasedRelation, right: Table) =>
          val leftRel = getTable(left)
          val rightName = right.getName.toString
          (left.getAlias.toString, leftRel.getName.toString) :: (rightName, rightName) :: Nil
        case (left: Table, right: AliasedRelation) =>
          val leftName = left.getName.toString
          val rightRel = getTable(right)
            (leftName, leftName) :: (right.getAlias.toString, rightRel.getName.toString) :: Nil
        case (left: AliasedRelation, right: Join) =>
          val leftRel = getTable(left)
          (left.getAlias.toString, leftRel.getName.toString) :: proc(right)
        case (left: Table, right: Table) =>
          val leftName = left.getName.toString
          val rightName = right.getName.toString
          (leftName, leftName) :: (rightName, rightName) :: Nil
        case (left: Table, right: Join) =>
          val leftName = left.getName.toString
          (leftName, leftName) :: proc(right)
        case (_, _) => throw new Error(s"Joins are only allowed on aliased tables")
      }
    }
    val lst = proc(tree)
    if (lst.groupBy(_._1).exists{ case (k, v) => v.size > 1 })
      throw new Error("There are duplicate aliases/tables in FROM clause")
    else
      lst.toMap
  }

  def getMeta(meta: Map[String, TableMetaData], name: String) = {
    meta get name match {
      case None => throw new Error(s"""Relation \"${name}\" does not exist""")
      case Some(x) => x
    }
  }
  // TODO: Add check if column acutally exists
  def giveAlias(expr: Expr, alias: String, fun: ((String, String) => String)): Expr = {
    expr match {
      case DerefExp(newAlias, name) => DerefExp(newAlias, fun(newAlias, name))
      case Attribute(name) => DerefExp(alias, fun(alias, name))
      case Less(left, right) => Less(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Leq(left, right) => Leq(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Greater(left, right) => Greater(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Geq(left, right) => Geq(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Equals(left, right) => Equals (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Plus(left, right) => Plus (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Minus(left, right) => Minus(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case And(left, right) => And(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Or(left, right) => Or (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Not(value) => Not(giveAlias(value, alias, fun))
      case x => x
    }
  }
  // def checkExpr(expr: Expr): Unit = {
  //   expr match {
  //     case Attribute(name) => throw new Error(s"Alias is needed on a reference to column $name")
  //     case x: BinOp => checkExpr(x.left); checkExpr(x.right)
  //     case Not(value) => checkExpr(value)
  //     case _ => {}
  //   }
  // }
  def joinAlias(expr: Expr, fun: (String => String), fun1: ((String,String) => String)): Expr = {
    expr match {
      case DerefExp(alias, attName) => DerefExp(alias, (fun1(alias,attName)))
      case Attribute(name) => DerefExp(fun(name), name)
      case Less(left, right) => Less(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Leq(left, right) => Leq(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Greater(left, right) => Greater(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Geq(left, right) => Geq(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Equals(left, right) => Equals (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Plus(left, right) => Plus (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Minus(left, right) => Minus(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case And(left, right) => And(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Or(left, right) => Or (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Not(value) => Not(joinAlias(value, fun, fun1))
      case x => x
    }
  }
  def checkAmbiguity(name: String) (tables: Map[String, Map[String, DataType]])  = {
    val map = tables.map{ case (k, m) => (k -> m.contains(name)) }
    val numTable = map.count{ case (k,v) => v }
    numTable match {
      case 1 => (map.filter{ case (k, v) => v }).head._1
      case n if n > 1 => throw new Error(s"""Column reference \"${name}\" is ambiguous""")
      case _ => throw new Error(s"""Column \"${name}\" does not exist""")
    }
  }
  def filterMap(tableMap: Map[String, TableMetaData], aliasMap: Map[String, String]) = {
    def proc(name: String): Map[String, DataType] = {
      tableMap get name match {
        case None => throw new Error(s"Table ${name} does not exist")
        case Some(meta) => meta.attributes
      }
    }
    aliasMap.map{ case (k, v) => (k -> proc(v)) }
  }
  def checkAlias(nameAlias: (String, String))(map: Map[String, Map[String, DataType]]) = {
    val (alias, name) = nameAlias
    map get alias match {
      case None => throw new Error(s"""Unknown alias \"${alias}\"""")
      case Some(map) => map get name match {
        case None => throw new Error(s"""Column \"${alias}.${name}\" does not exist""")
        case Some(_) => name
      }
    }
  }

  def apply(query: Query, meta: Map[String, TableMetaData]) = {
    def genPlan(whereCond: Option[Expr], name: String, alias: String) = {
      whereCond match {
        case Some(expr) =>
          val (operator, cond) = translateCond(expr, getMeta(meta, name), alias)
          (operator, cond)
        case None =>
          val tMeta = getMeta(meta, name)
          val types = tMeta.indexParts.map { case x => (tMeta.attributes get x).get }
          (RangeScan(tMeta,
            (types zip tMeta.indexParts.map { case c => ZeroVal() }),
            (types zip tMeta.indexParts.map { case c => MaxVal() }),
            alias), None)
      }
    }
    def genQList(whereCond: Option[Expr], aliasLst: List[(String, String)]):
        (List[Physical], Option[Expr]) = {
      aliasLst match {
        case x::xs =>
          val (plan, newCond) = genPlan(whereCond, x._2, x._1)
          val (recLst, recCond) = genQList(newCond, xs)
          (plan :: recLst, recCond)
        case _ => (Nil, whereCond)
      }
    }
    val body = query.getQueryBody.asInstanceOf[QuerySpecification]
    val projList = body.getSelect.getSelectItems.asScala.toList match {
      case List(c: AllColumns) => Nil
      case c => c.map{ case x => parseExp(x.asInstanceOf[SingleColumn].getExpression) }
    }
    val whereCond = toScala(body.getWhere) match {
      case None => None
      case Some(whr) => Some(parseExp(whr))
    }
    toScala(body.getFrom).get match {
      case x: Table =>
        val name = x.getName.toString
        val newMap = filterMap(meta, Map(name -> name))
        // checkAlias(_: String, _:String)(newMap)
        val aliasedProj = projList.map {
          case x => giveAlias(x, name, checkAlias(_:String, _:String)(newMap))
        }
        val cond = whereCond match {
          case Some(expr) => Some(giveAlias(expr, name, checkAlias(_:String, _:String)(newMap)))
          case None => None
        }
        val (operation, newCond) = genPlan(cond, name, name)
        JaqcoQuery(operation, newCond, aliasedProj)
      case x: AliasedRelation =>
        val name = getTable(x).getName.toString
        val alias = x.getAlias.toString
        val newMap = filterMap(meta, Map(alias -> name))
        val aliasedProj = projList.map {
          case x => giveAlias(x, alias, checkAlias(_:String, _:String)(newMap))
        }
        val cond = whereCond match {
          case Some(expr) => Some(giveAlias(expr, alias, checkAlias(_:String, _:String)(newMap)))
          case None => None
        }
        val (operation, newCond) = genPlan(cond, getTable(x).getName.toString, x.getAlias.toString)
        JaqcoQuery(operation, newCond, aliasedProj)
      case x: Join =>
        val aliasMap = x.getType.toString match {
          case "IMPLICIT" => parseJoin(x)
          case joinType => throw new Error(s"$joinType JOIN is not supported")
        }
        val newMap = filterMap(meta, aliasMap)
        val aliasedProj = projList.map {
          case expr =>
            joinAlias(
              expr,
              (checkAmbiguity(_)(newMap)),
              (checkAlias(_: String, _:String)(newMap))
            )
        }
        val cond = whereCond match {
          case Some(expr) =>
            Some(
              joinAlias(
                expr,
                (checkAmbiguity(_)(newMap)),
                (checkAlias(_: String, _:String)(newMap))
              )
            )
          case None => None
        }
        val (lst, newCond) = genQList(cond, aliasMap.toList)
        JaqcoQuery(ThetaJoin(lst), newCond, aliasedProj)
      case x => throw new Error(s"Selection from $x is not supported yet")
    }
  }
}
