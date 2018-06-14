package jaqco
case class TableMetaData(relName: String, indexParts: Map[String, DataType], attributes: Map[String, DataType])

sealed trait Physical
case class RangeScan(
  meta: TableMetaData,
  from: List[(DataType, RangeVal)],
  to: List[(DataType, RangeVal)],
  alias: String,
  projList: List[Expr],
  selectExpr: Option[Expr]
) extends Physical

case class IndexLookup(meta: TableMetaData, map: Map[String, Expr], alias: String) extends Physical
case class OnePassProj(projList: List[Expr], source: Physical) extends Physical
case class NestedLoopJoin(tables: List[Physical], joinCond: Option[Expr]) extends Physical
case class Filter(cond: Expr, source: Physical) extends Physical

import OperatorGenerator._
import Utils._

object PhysicalPlanGenerator{
  def apply(tree: RelAlg, meta: Map[String, TableMetaData]) = {
    def genPlan(whereCond: Option[Expr], name: String, alias: String) = {
      whereCond match {
        case Some(expr) =>
          val (operator, cond) = translateCond(expr, getMeta(meta, name), alias)
          (operator, cond)
        case None =>
          // No where clause - do the range scan from min to max key
          val tableMeta = getMeta(meta, name)
          // Add aliases to the table meta
          val tMeta = {
            val attributes = tableMeta.attributes.map { case x => (alias + "_" + x._1 -> x._2) }
            // val indexParts = tableMeta.indexParts.map { case x => (alias + "_" + x) }
            val indexParts = tableMeta.indexParts
            TableMetaData(tableMeta.relName, indexParts, attributes)
          }
          // val types = tMeta.indexParts.map { case x => (tMeta.attributes get x).get }
          val types = tMeta.indexParts.map { case x => x._2 }.toList
          (RangeScan(tMeta,
            (types zip tMeta.indexParts.map { case c => ZeroVal() }),
            (types zip tMeta.indexParts.map { case c => MaxVal() }),
            alias,
            Nil,
            None
          ), None)
      }
    }
    // def findForeignTypes(): Expr = {

    // }
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
    val (projList, selection) = tree match {
      case Projection(lst, x) => (lst, x)
      case x => (Nil, x)
    }
    val (whereCond, from) = selection match {
      // case Selection(cond, x) => (Some(cond), x)
      case Selection(cond, x) => (Some(findForeignTypes(cond, meta, SimpleType("bool"))), x)
      case x => (None, x)
    }
    from match {
      case Rel(name, alias) =>
        val newMap = filterMap(meta, Map(alias -> name))
        // Give alias to column references in the projection list
        val aliasedProj = projList.map {
          case x => giveAlias(x, alias, checkAlias(_:String, _:String)(newMap))
        }
        // Give alias to column references in WHERE clause
        val cond = whereCond match {
          case Some(expr) => Some(giveAlias(expr, alias, checkAlias(_:String, _:String)(newMap)))
          case None => None
        }
        val (operation, newCond) = genPlan(cond, name, alias)
        // Add projection to the range scan
        val newOp = operation match {
          case RangeScan(m, f, t, a, _ , s) => RangeScan(m, f, t, a, aliasedProj, s)
          case x => newCond match {
            case None => x
            case Some(cond) => Filter(cond, x)
          }
        }
        // Add filter if needed
        projList match {
          case Nil => newOp
          case x => OnePassProj(aliasedProj, newOp)
        }
      case x:Cross =>
        val aliasMap = parseJoin(x)
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
        projList match {
          case Nil => NestedLoopJoin(lst, newCond)
          case _ => OnePassProj(aliasedProj, NestedLoopJoin(lst, newCond))
        }
      case _ => throw new Error("This from clause is not supported")
    }
  }
  def parseJoin(tree: Cross): Map[String, String] = {
    def proc(tree: Cross): List[(String, String)] = {
      tree match {
        case Cross(Rel(nameLeft, aliasLeft), Rel(nameRight, aliasRight)) =>
          (aliasLeft, nameLeft) :: (aliasRight, nameRight) :: Nil
        case Cross(Rel(name, alias), right:Cross) =>
          (alias, name) :: proc(right)
        case _ => throw new Error(s"That type of join is not implemented yet")
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
  //TODO: Ugly, implement with lenses
  def giveAlias(expr: Expr, alias: String, fun: ((String, String) => String)): Expr = {
    expr match {
      case DerefExp(newAlias, name) => DerefExp(newAlias, fun(newAlias, name))
      case Attribute(name)      => DerefExp(alias, fun(alias, name))
      case Less(left, right)    => Less(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Leq(left, right)     => Leq(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Greater(left, right) => Greater(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Geq(left, right)     => Geq(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Equals(left, right)  => Equals (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Plus(left, right)    => Plus (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Minus(left, right)   => Minus(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Mult(left, right)    => Mult(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Mod(left, right)     => Mod(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case UnaryMinus(value)    => UnaryMinus(giveAlias(value, alias, fun))
      case Div(left, right)     => Div(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case And(left, right)     => And(giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Or(left, right)      => Or (giveAlias(left, alias, fun), giveAlias(right, alias, fun))
      case Not(value)           => Not(giveAlias(value, alias, fun))
      case x => x
    }
  }
  //TODO: Ugly, implement with lenses
  def joinAlias(expr: Expr, fun: (String => String), fun1: ((String,String) => String)): Expr = {
    expr match {
      case DerefExp(alias, attName) => DerefExp(alias, (fun1(alias,attName)))
      case Attribute(name)      => DerefExp(fun(name), name)
      case Less(left, right)    => Less(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Leq(left, right)     => Leq(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Greater(left, right) => Greater(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Geq(left, right)     => Geq(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Equals(left, right)  => Equals (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Plus(left, right)    => Plus (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Minus(left, right)   => Minus(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Mult(left, right)    => Mult(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Mod(left, right)     => Mod(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case UnaryMinus(value)    => UnaryMinus(joinAlias(value, fun, fun1))
      case Div(left, right)     => Div(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case And(left, right)     => And(joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Or(left, right)      => Or (joinAlias(left, fun, fun1), joinAlias(right, fun, fun1))
      case Not(value)           => Not(joinAlias(value, fun, fun1))
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
}
