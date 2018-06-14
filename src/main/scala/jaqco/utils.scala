package jaqco
case class SimpleRef(counter: Int, fun: (Int => Unit))
object Utils {
  def isConstExpr (expr: Expr, meta: TableMetaData): Boolean = {
    expr match {
      case x: BinOp => isConstExpr(x.left, meta) && isConstExpr(x.right, meta)
      case _ @ (_: Const | _: OutsideVar) => true
      case _ => false
    }
  }
  def checkDuplicates(lst: List[String], what: String) = {
    lst.groupBy(identity).filter{ case x => (x._1 != "") && (x._2.size > 1) }.headOption match {
      case Some((name, _)) => throw new Error(s"""$what \"$name\" is given twice""")
      case None => {}
    }
  }
  // TODO: Ugly as sin. Needs to be rewritten completely at some point
  def findForeignTypes(expr: Expr, meta: Map[String, TableMetaData], singleType: DataType): Expr = {
    def proc(expr: Expr): (DataType, Expr)  = {
      def arithmetic(x: BinOp) = {
        (x.left, x.right) match {
          case (OutsideVar(leftName, _), OutsideVar(rightName, _)) =>
            throw new Error(s"Can not infer the type of a shared variable ${leftName}")
          case (OutsideVar(name, _), right) =>
            val (rightType, rightExpr) = proc(right)
            (rightType, OutsideVar(name, rightType), rightExpr)
          case (left, OutsideVar(name, _)) =>
            val (leftType, leftExpr) = proc(left)
            (leftType, leftExpr, OutsideVar(name, leftType))
          case (left, right) =>
            val (leftType, leftExpr) = proc(left)
            val (rightType, rightExpr) = proc(right)
            (leftType, leftExpr, rightExpr)
        }
      }
      // def comparison(x: RangeVal) = {
      //   (x.left, x.right) match {
      //     case (OutsideVar(leftName, _), OutsideVar(rightName, _)) =>
      //       (OutsideVar(leftName, SimpleType("std::int64_t")),
      //         OutsideVar(rightName, SimpleType("std::int64_t"))
      //     case (OutsideVar(name, _), right) =>
      //       val (rightType, rightExpr) = proc(right)
      //       (OutsideVar(name, rightType), rightExpr)
      //     case (left, OutsideVar(name, _)) =>
      //       val (leftType, leftExpr) = proc(left)
      //       (leftExpr, OutsideVar(name, leftType))
      //     case (left, right) =>
      //       val (leftType, leftExpr) = proc(left)
      //       val (rightType, rightExpr) = proc(right)
      //       (leftExpr, rightExpr)
      //   }
      // }
      expr match {
        case x:Geq =>
          val (_, left, right) = arithmetic(x)
          (SimpleType("bool"), Geq(left, right))
        case x:Greater =>
          val (_, left, right) = arithmetic(x)
          (SimpleType("bool"), Greater(left, right))
        case x:Leq =>
          val (_, left, right) = arithmetic(x)
          (SimpleType("bool"), Leq(left, right))
        case x:Less =>
          val (_, left, right) = arithmetic(x)
          (SimpleType("bool"), Less(left, right))
        case x:Equals =>
          val (_, left, right) = arithmetic(x)
          (SimpleType("bool"), Equals(left, right))
        case x:Mult =>
          val (typ, left, right) = arithmetic(x)
          (typ, Mult(left, right))
        case x:Plus =>
          val (typ, left, right) = arithmetic(x)
          (typ, Plus(left, right))
        case x:Mod =>
          val (typ, left, right) = arithmetic(x)
          (typ, Mod(left, right))
        case x:Minus =>
          val (typ, left, right) = arithmetic(x)
          (typ, Minus(left, right))
        case x:Div =>
          val (typ, left, right) = arithmetic(x)
          (typ, Minus(left, right))
        case And(left, right) =>
          val (_, leftExpr) = proc(left)
          val (_, rightExpr) = proc(right)
          (SimpleType("bool"), And(leftExpr, rightExpr))
        case Or(left, right) =>
          val (_, leftExpr) = proc(left)
          val (_, rightExpr) = proc(right)
          (SimpleType("bool"), Or(leftExpr, rightExpr))
        case UnaryMinus(OutsideVar(name, _)) =>
          throw new Error(s"Can not infer the type of a shared variable ${name}")
        case UnaryMinus(insExpr) =>
          val (resType, resExpr) = proc(insExpr)
          (resType, UnaryMinus(resExpr))
        case Not(OutsideVar(name, _)) => (SimpleType("bool"), Not(OutsideVar(name, SimpleType("bool"))))
        case Not(insExpr) =>
          val (_, resExpr) = proc(insExpr)
          (SimpleType("bool"),Not(resExpr))
        case Const(name, varType) => (varType, expr)
        case OutsideVar(name, _)  => (singleType, OutsideVar(name, singleType))
        case Attribute(name) =>
          val get = meta.map { case x => x._2.attributes get name }.partition(_ != None)._1.headOption
          get match {
            case None => throw new Error(s"Unknow refference to $name attribute")
            case Some(x) => (x.get, expr)
          }
        case _ => throw new Error("Type inference failed")
      }
    }
    proc(expr)._2
  }
}
