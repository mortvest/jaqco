sealed trait Expr
case class Attribute(attName: String) extends Expr
case class Less(left: Expr, right: Expr) extends Expr
case class Leq(left: Expr, right: Expr) extends Expr
case class Greater(left: Expr, right: Expr) extends Expr
case class Geq(left: Expr, right: Expr) extends Expr
case class Equals(left: Expr, right: Expr) extends Expr
case class Plus(left: Expr, right: Expr) extends Expr
case class Minus(left: Expr, right: Expr) extends Expr
case class And(left: Expr, right: Expr) extends Expr
case class Or(left: Expr, right: Expr) extends Expr
case class Not(value: Expr) extends Expr

// TODO: Add types floats, dates etc
sealed trait Const extends Expr
case class LongConst(value: Long) extends Const
case class StringConst(value: String) extends Const

sealed trait RelAlg
case class Relation(relName: String) extends RelAlg
case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
case class Selection(cond: Expr, from: RelAlg) extends RelAlg
case class Cross(left: RelAlg, right: RelAlg) extends RelAlg
case class NaturalJoin(left: RelAlg, right: RelAlg) extends RelAlg

import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

object RelAlg {
  def parseExp(tree: Expression): Expr = {
    tree match {
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
      case x: Identifier => Attribute(x.getValue.toString)
      case x: LongLiteral => LongConst(x.getValue)
      case x: StringLiteral => StringConst(x.getValue)
    }
  }

  def apply(query: Query) = {
    val body = query.getQueryBody.asInstanceOf[QuerySpecification]
    val where = toScala(body.getWhere)
    val from = Relation(toScala(body.getFrom).get.asInstanceOf[Table].getName.toString)
    // // TODO: add Cross/Join support i select list
    // val fromOp = toScala(body.getFrom).get
    // println(fromOp)
    // val from = fromOp match {
    //   case x if x.isInstanceOf[Table] => Relation(x.asInstanceOf[Table].getName.toString)
    //   case x if x.isInstanceOf[Join] =>
    //     val join = x.asInstanceOf[Join]
    //     val joinType = join.getType.toString
    //     val right = join.getType.toString
    // }
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
}
