// // final case class Error(private val message: String = "",
// //   private val cause: Throwable = None.orNull) extends Exception(message, cause)

// // sealed trait Expr
// // abstract class BinOp extends Expr {
// //   def left: Expr
// //   def right: Expr
// // }
// // abstract class UnaryOp extends Expr {
// //   def value: Expr
// // }
// // abstract class RangeOp extends BinOp
// // case class DerefExp(alias: String, columnName: String) extends Expr
// // case class Attribute(attName: String) extends Expr
// // case class OutsideVar(name: String, varType: DataType) extends Expr
// // // case class OutsideVar(name: String) extends Expr
// // // case class Less(left: Expr, right: Expr) extends RangeOp
// // // case class Leq(left: Expr, right: Expr) extends RangeOp
// // // case class Greater(left: Expr, right: Expr) extends RangeOp
// // // case class Geq(left: Expr, right: Expr) extends RangeOp
// // case class Equals(left: Expr, right: Expr) extends BinOp
// // case class Plus(left: Expr, right: Expr) extends BinOp
// // case class Minus(left: Expr, right: Expr) extends BinOp
// // case class Mult(left: Expr, right: Expr) extends BinOp
// // case class Div(left: Expr, right: Expr) extends BinOp
// // case class Mod(left: Expr, right: Expr) extends BinOp
// // case class UnaryMinus(value: Expr) extends UnaryOp
// // case class And(left: Expr, right: Expr) extends BinOp
// // case class Or(left: Expr, right: Expr) extends BinOp
// // case class Not(value: Expr) extends UnaryOp
// // case class Const(value: String, varType: DataType) extends Expr
// // // TODO: Add types floats, dates etc

// // sealed trait DataType
// // case class SimpleType(typeName: String) extends DataType
// // case class StringType(size: Int) extends DataType
// // case class NoType() extends DataType

// // sealed trait RangeVal
// // case class ConstVal(expr: Expr) extends RangeVal
// // case class ZeroVal() extends RangeVal
// // case class MaxVal() extends RangeVal

// // sealed trait RelAlg
// // case class Rel(relName: String, alias: String) extends RelAlg
// // case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
// // case class Selection(cond: Expr, from: RelAlg) extends RelAlg
// // case class Cross(left: RelAlg, right: RelAlg) extends RelAlg

// // import shapeless._
// // import syntax.typeable._
// import shapeless._, ops.hlist._, ops.record._

// object Test extends App {
//   def findForeignTypes(expr: Expr, meta: Map[String, TableMetaData]): Expr = {
//     def proc(expr: Expr): (DataType, Expr)  = {
//       def foo[T <: BinOp] (x: T) = {
//         case class Tmp(left: Expr, right: Expr) extends BinOp
//           (x.left, x.right) match {
//           case (OutsideVar(leftName, _), OutsideVar(rightName, _)) =>
//             throw new Error(s"Can not infer the type of a shared variable ${leftName}")
//           case (OutsideVar(name, _), right) =>
//             val (rightType, rightExpr) = proc(right)
//             (rightType, OutsideVar(name, rightType), rightExpr)
//           case (left, OutsideVar(name, _)) =>
//             val (leftType, leftExpr) = proc(left)
//             (leftType, leftExpr, OutsideVar(name, leftType))
//           case (left, right) =>
//             val (leftType, leftExpr) = proc(left)
//             val (rightType, rightExpr) = proc(right)
//             (leftType, leftExpr, rightExpr)
//         }
//       }
//       expr match {
//         case x:Geq =>
//           val (_, left, right) = foo(x)
//           (SimpleType("bool"), Geq(left, right))
//         case x:Greater =>
//           val (_, left, right) = foo(x)
//           (SimpleType("bool"), Greater(left, right))
//         case x:Leq =>
//           val (_, left, right) = foo(x)
//           (SimpleType("bool"), Leq(left, right))
//         case x:Less =>
//           val (_, left, right) = foo(x)
//           (SimpleType("bool"), Less(left, right))
//         case x:Equals =>
//           val (_, left, right) = foo(x)
//           (SimpleType("bool"), Equals(left, right))
//         case x:Mult =>
//           val (typ, left, right) = foo(x)
//           (typ, Mult(left, right))
//         case x:Plus =>
//           val (typ, left, right) = foo(x)
//           (typ, Plus(left, right))
//         case x:Mod =>
//           val (typ, left, right) = foo(x)
//           (typ, Mod(left, right))
//         case x:Minus =>
//           val (typ, left, right) = foo(x)
//           (typ, Minus(left, right))
//         case x:Div =>
//           val (typ, left, right) = foo(x)
//           (typ, Minus(left, right))
//         case And(left, right) =>
//           val (_, leftExpr) = proc(left)
//           val (_, rightExpr) = proc(right)
//           (SimpleType("bool"), And(leftExpr, rightExpr))
//         case Or(left, right) =>
//           val (_, leftExpr) = proc(left)
//           val (_, rightExpr) = proc(right)
//           (SimpleType("bool"), Or(leftExpr, rightExpr))
//         case UnaryMinus(OutsideVar(name, _)) =>
//           throw new Error(s"Can not infer the type of a shared variable ${name}")
//         case UnaryMinus(insExpr) =>
//           val (resType, resExpr) = proc(insExpr)
//           (resType, UnaryMinus(resExpr))
//         case Not(OutsideVar(name, _)) => (SimpleType("bool"), Not(OutsideVar(name, SimpleType("bool"))))
//         case Not(insExpr) =>
//           val (_, resExpr) = proc(insExpr)
//           (SimpleType("bool"),Not(resExpr))
//         case Const(name, varType) => (varType, expr)
//         case OutsideVar(name, _)  => (SimpleType("bool"), OutsideVar(name, SimpleType("bool")))
//         case Attribute(name) =>
//           val get = meta.map { case x => x._2.attributes get name }.partition(_ != None)._1.headOption
//           get match {
//             case None => throw new Error(s"Unknow refference to $name attribute")
//             case Some(x) => (x.get, expr)
//           }
//       }
//     }
//     proc(expr)._2
//   }

//   val meta = Map[String, TableMetaData](
//     "test_relation" ->
//       TableMetaData("test_relation", Map("user_name" -> StringType(255)),
//         Map("user_name" -> StringType(255),
//           "account_id" -> SimpleType("long"),
//           "balance" -> SimpleType("long")
//         ))
//   )
//   println(
//     findForeignTypes(
//       Not(
//         Equals(
//           Const("1", SimpleType("long")),
//           Plus(Const("1", SimpleType("long")), Attribute("account_id"))
//         )
//       ),
//       meta
//     )
//   )
//   // println(foo(Minus(Const("1", SimpleType("int")), OutsideVar("var", NoType()))))
// }

