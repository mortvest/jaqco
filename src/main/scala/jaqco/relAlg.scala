package jaqco
final case class Error(private val message: String = "",
  private val cause: Throwable = None.orNull) extends Exception(message, cause)

sealed trait Expr
abstract class BinOp extends Expr {
  def left: Expr
  def right: Expr
}
abstract class UnaryOp extends Expr {
  def value: Expr
}
abstract class RangeOp extends BinOp
case class DerefExp(alias: String, columnName: String) extends Expr
case class Attribute(attName: String) extends Expr
case class OutsideVar(name: String, varType: DataType) extends Expr
// case class OutsideVar(name: String) extends Expr
case class Less(left: Expr, right: Expr) extends RangeOp
case class Leq(left: Expr, right: Expr) extends RangeOp
case class Greater(left: Expr, right: Expr) extends RangeOp
case class Geq(left: Expr, right: Expr) extends RangeOp
case class Equals(left: Expr, right: Expr) extends BinOp
case class Plus(left: Expr, right: Expr) extends BinOp
case class Minus(left: Expr, right: Expr) extends BinOp
case class Mult(left: Expr, right: Expr) extends BinOp
case class Div(left: Expr, right: Expr) extends BinOp
case class Mod(left: Expr, right: Expr) extends BinOp
case class UnaryMinus(value: Expr) extends UnaryOp
case class And(left: Expr, right: Expr) extends BinOp
case class Or(left: Expr, right: Expr) extends BinOp
case class Not(value: Expr) extends UnaryOp
case class Const(value: String, varType: DataType) extends Expr
// TODO: Add types floats, dates etc

sealed trait DataType
case class SimpleType(typeName: String) extends DataType
case class StringType(size: Int) extends DataType
case class NoType() extends DataType

sealed trait RangeVal
case class ConstVal(expr: Expr) extends RangeVal
case class ZeroVal() extends RangeVal
case class MaxVal() extends RangeVal

sealed trait RelAlg
case class Rel(relName: String, alias: String) extends RelAlg
case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
case class Selection(cond: Expr, from: RelAlg) extends RelAlg
case class Cross(left: RelAlg, right: RelAlg) extends RelAlg

import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

object LogicalPlanGenerator{
  def apply(query: Query) = {
    val body = query.getQueryBody.asInstanceOf[QuerySpecification]
    val where = toScala(body.getWhere)
    checkClause(toScala(body.getGroupBy))
    checkClause(toScala(body.getHaving))
    checkClause(toScala(body.getOrderBy))
    val from = toScala(body.getFrom).get match {
      case x: Join => x.getType.toString match {
        case "IMPLICIT" => parseJoin(x)
        case joinType => throw new Error(s"$joinType JOIN is not supported")
      }
      case x => parseTable(x)
    }
    val whereCond =
      where match {
        case Some(whr) => Selection(parseExp(whr), from)
        case None => from
      }
    val columns = body.getSelect.getSelectItems.asScala.toList
    columns match {
      case List(c: AllColumns) => whereCond
      case c =>
        Projection(c.map({case x => parseExp(x.asInstanceOf[SingleColumn].getExpression)}),whereCond)
    }
  }
  def checkClause(opt: Option[Node]) = {
    opt match {
      case Some(_) => throw new Error(s"$opt clause is not supported")
      case None => {}
    }
  }
  def findOutsideVar(name: String): Expr = {
    val outsidePattern = "_(.+)".r
    name match {
      case outsidePattern(varName) => OutsideVar(varName, NoType())
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
        ex.getType.getValue match {
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
          case "-" => Minus(left, right)
          case "*" => Mult(left, right)
          case "/" => Div(left, right)
          case "%" => Mod(left, right)
          // TODO: moar ops
          case _ => throw new Error(s"Operator ${compType} is not supported")
        }
      case ex: ArithmeticUnaryExpression =>
        val value = parseExp(ex.getValue)
        ex.getSign.toString match {
          case "PLUS" => value
          case "MINUS" => UnaryMinus(value)
        }
      case ex: LogicalBinaryExpression =>
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        ex.getType.toString match {
          case "AND" => And(left, right)
          case "OR" => Or(left, right)
        }
      case x: NotExpression => Not(parseExp(x.getValue))
      case x: Identifier => findOutsideVar(x.getValue.toString)
      case x: LongLiteral => Const(x.getValue.toString, SimpleType("std::int64_t"))
      case x: DecimalLiteral => Const(x.getValue.toString, SimpleType("double"))
      case x: BooleanLiteral => Const(x.getValue.toString, SimpleType("bool"))
      case x: StringLiteral =>
        val str = x.getValue.toString
        Const(str, StringType(str.size))
    }
  }
  def parseTable(rel: Relation) = {
    rel match {
      case x: Table =>
        val name = x.getName.toString
        Rel(name, name)
      case x: AliasedRelation =>
        Rel(x.getRelation.asInstanceOf[Table].getName.toString, x.getAlias.toString)
      case _ => throw new Error("This type of join is not supported")
    }
  }

  def parseJoin(tree: Join): Cross = {
    (tree.getLeft, tree.getRight) match {
      case (left: Relation, right: Join) => Cross(parseTable(left), parseJoin(right))
      case (left: Relation, right: Relation) => Cross(parseTable(left), parseTable(right))
      case _ => throw new Error(s"This type of join is not supported")
    }
  }
}
