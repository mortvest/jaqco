sealed trait Expr
case class Attribute(attName: String) extends Expr
case class LongConst(value: Long) extends Expr
case class IntConst(value: Int) extends Expr
case class StringConst(value: String) extends Expr
case class Less(left: Expr, right: Expr) extends Expr
case class Leq(left: Expr, right: Expr) extends Expr
case class Greater(left: Expr, right: Expr) extends Expr
case class Geq(left: Expr, right: Expr) extends Expr
case class Equals(left: Expr, right: Expr) extends Expr
case class Plus(left: Expr, right: Expr) extends Expr
case class Minus(left: Expr, right: Expr) extends Expr
case class And(left: Expr, right: Expr) extends Expr
case class Or(left: Expr, right: Expr) extends Expr

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

object Test extends App {
  val parser = new SqlParser()
  val query = parser.createStatement("SELECT a, b, 11 FROM account WHERE a <= 'david' AND b <= c")
  query match {
    // TODO: Better way for pattern matching on types
    case q if q.isInstanceOf[Query]  => println(parseSelect(q.asInstanceOf[Query]))
    case q if q.isInstanceOf[Delete] => println("This is a delete!")
    case q if q.isInstanceOf[Insert] => println("This is an insert!")
    case _ => println("Operator is not supported")
  }

  def parseExp(tree: Expression): Expr = {
    tree match {
      case x if x.isInstanceOf[ComparisonExpression] =>
        val ex = x.asInstanceOf[ComparisonExpression]
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
      case x if x.isInstanceOf[ArithmeticBinaryExpression] =>
        val ex = x.asInstanceOf[ArithmeticBinaryExpression]
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        val compType = ex.getType.getValue
        compType match {
          case "+" => Plus(left, right)
          // TODO: moar ops
          case _ => Minus(left, right)
        }
      case x if x.isInstanceOf[LogicalBinaryExpression] =>
        val ex = x.asInstanceOf[LogicalBinaryExpression]
        val left = parseExp(ex.getLeft)
        val right = parseExp(ex.getRight)
        val compType = ex.getType.toString
        compType match {
          case "AND" => And(left, right)
          case "OR" => Or(left, right)
        }
      case x if x.isInstanceOf[Identifier] =>
        val ex = x.asInstanceOf[Identifier]
        val name = ex.getValue.toString
        Attribute(name)
      case x if x.isInstanceOf[LongLiteral] =>
        val ex = x.asInstanceOf[LongLiteral]
        val value = ex.getValue
        LongConst(value)
      case x if x.isInstanceOf[StringLiteral] =>
        val ex = x.asInstanceOf[StringLiteral]
        val value = ex.getValue
        StringConst(value)
    }
  }

  def parseSelect(query: Query) = {
    def transSelect (lst: List[SelectItem]): List[Expr] = {
      lst match {
        case x::xs if x.asInstanceOf[SingleColumn].getExpression.isInstanceOf[LongLiteral] =>
          parseExp(x.asInstanceOf[SingleColumn].getExpression) :: transSelect(xs)
        case x::xs if x.asInstanceOf[SingleColumn].getExpression.isInstanceOf[Identifier] =>
          parseExp(x.asInstanceOf[SingleColumn].getExpression) :: transSelect(xs)
        case Nil => Nil
      }
    }
    val body = query.getQueryBody.asInstanceOf[QuerySpecification]
    val select = body.getSelect
    val where = toScala(body.getWhere)
    val from = toScala(body.getFrom).get.asInstanceOf[Table].getName.toString
    val columns = select.getSelectItems.asScala.toList
      (select, where) match {
      case (select, Some(whr)) =>
        Projection(transSelect(columns), Selection(parseExp(whr), Relation(from)))
      case (select, None) =>
        columns match {
          case List(c) if c.isInstanceOf[AllColumns] => Relation(from)
          case c => Projection(transSelect(columns), Relation(from))
        }
    }
  }
}
