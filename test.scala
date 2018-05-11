// sealed trait Expr
// abstract class BinOp extends Expr {
//   def left: Expr
//   def right: Expr
// }
// abstract class RangeOp extends BinOp
// case class Attribute(attName: String) extends Expr
// case class OutsideVar(name: String, varType: DataType) extends Expr
// case class Less(left: Expr, right: Expr) extends RangeOp
// case class Leq(left: Expr, right: Expr) extends RangeOp
// case class Greater(left: Expr, right: Expr) extends RangeOp
// case class Geq(left: Expr, right: Expr) extends RangeOp
// case class Equals(left: Expr, right: Expr) extends BinOp
// case class Plus(left: Expr, right: Expr) extends BinOp
// case class Minus(left: Expr, right: Expr) extends BinOp
// case class And(left: Expr, right: Expr) extends BinOp
// case class Or(left: Expr, right: Expr) extends BinOp
// case class Not(value: Expr) extends Expr

// // TODO: Add types floats, dates etc
// // sealed trait Const extends Expr
// case class Const(value: String, varType: DataType) extends Expr

// sealed trait DataType
// case class SimpleType(typeName: String) extends DataType
// case class StringType(size: Int) extends DataType

// sealed trait RelAlg
// case class Relation(relName: String) extends RelAlg
// case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
// case class Selection(cond: Expr, from: RelAlg) extends RelAlg
// case class Cross(left: RelAlg, right: RelAlg) extends RelAlg
// case class NaturalJoin(left: RelAlg, right: RelAlg) extends RelAlg
// final case class Error(private val message: String = "",
//   private val cause: Throwable = None.orNull) extends Exception(message, cause)

// case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, DataType])

// sealed trait RangeVal
// case class ConstVal(expr: Expr) extends RangeVal
// case class ZeroVal() extends RangeVal
// case class MaxVal() extends RangeVal

// sealed trait Physical
// case class RangeScan(meta: TableMetaData, from: List[RangeVal], to: List[RangeVal]) extends Physical
// case class IndexLookup(meta: TableMetaData, map: Map[String, Expr]) extends Physical
// case class Filter(expr: Expr, from: Physical) extends Physical
// case class OnePassProj(exprList: List[Expr], from: Physical) extends Physical

// case class RelationMetaData(attributes: Map[String, DataType], name: String, typeName: String)

// case class Ref(relNum: Int, counter: Int, fun: (Int => Unit))

// import scala.collection.mutable.HashMap
// object x extends App {
//   def typeLookup(expr: Expr, meta: RelationMetaData): DataType = {
//     def checkBin(left: Expr, right: Expr) = {
//       val l = typeLookup(left, meta)
//       val r = typeLookup(right, meta)
//       if (l == r) l else throw new Error(s"Operands of $l and $r must be of the same type")
//     }
//     def checkBool(left: Expr, right: Expr, opName: String) = {
//       val l = typeLookup(left, meta)
//       val r = typeLookup(right, meta)
//       if (l == SimpleType("bool") && r == SimpleType("bool")) l
//       else throw new Error(s"Operands of ${opName} must be boolean")
//     }
//     def checkComparison(left: Expr, right: Expr) = {
//       val l = typeLookup(left, meta)
//       val r = typeLookup(right, meta)
//       if (l != r) throw new Error("Comparison is only allowed between values of the same type")
//       else SimpleType("bool")
//     }
//     expr match {
//       case Attribute(attName) => (meta.attributes get attName) match {
//         case None => throw new Error(s"Column $attName does not exist")
//         case Some(varType) => varType
//       }
//       case OutsideVar(name, varType) => varType
//       case Const(value, valType) => valType
//       case And(left, right) => checkBool(left, right, "AND")
//       case Or(left, right) => checkBool(left, right, "OR")
//       case x: RangeOp => checkComparison(x.left, x.right)
//       case Equals(left, right) => checkComparison(left, right)
//       case x: BinOp => checkBin(x.left, x.right)
//       case Not(expr) =>
//         val e = typeLookup(expr, meta)
//         if (e != SimpleType("bool")) throw new Error(s"Argument of NOT must be type boolean")
//         else e
//       case _ => throw new Error(s"Can not determine the type of expression")
//     }
//   }
//   def condTrans(expr: Expr, relMeta: RelationMetaData, structName: String): String = {
//     def trans(expr: Expr): String = {
//       expr match {
//         case Attribute(attName) =>
//           if (relMeta.attributes.keys.toList.contains(attName)) structName + "." + attName
//           else throw new Error(s"Column $attName does not exist")
//         case OutsideVar(name, _)  => name
//         case Less(left, right)    => "("  + trans(left) + " < "  + trans(right) + ")"
//         case Leq(left, right)     => "("  + trans(left) + " <= " + trans(right) + ")"
//         case Greater(left, right) => "("  + trans(left) + " > "  + trans(right) + ")"
//         case Geq(left, right)     => "("  + trans(left) + " >= " + trans(right) + ")"
//         case Equals(left, right)  => "("  + trans(left) + " == " + trans(right) + ")"
//         case Plus(left, right)    => "("  + trans(left) + " + "  + trans(right) + ")"
//         case Minus(left, right)   => "("  + trans(left) + " - "  + trans(right) + ")"
//         case And(left, right)     => "("  + trans(left) + " && " + trans(right) + ")"
//         case Or(left, right)      => "("  + trans(left) + " || " + trans(right) + ")"
//         case Not(value)           => "!(" + trans(value) + ")"
//         case Const(x, valType)    => valType match {
//           case SimpleType(_) => x
//           case StringType(_) => "\"" + x + "\""
//         }
//         case _  => throw new Error("Expression translation failed")
//       }
//     }
//     trans(expr)
//   }

//   def genMap(lst: List[Expr], structName: String, meta: RelationMetaData): List[(String, DataType, String, String, String)] = {
//     def fun(value: String, map: Map[String, Int]): (String, Map[String, Int]) = {
//       (map get value) match {
//         case None => (value, map + (value -> 1))
//         case Some(x) => (value + x.toString, map + (value -> (x + 1)))
//       }
//     }
//     def rec(lst: List[Expr], map: Map[String, Int]): List[(String, DataType, String, String, String)] = {
//       def proc(name: String, expr: Expr, lst: List[Expr]) = {
//         val value = condTrans(expr, meta, structName)
//         typeLookup(expr, meta) match {
//           case SimpleType(foundType) =>
//             val neu = if (name == "") fun(s"${foundType}_field", map) else fun(s"$name", map)
//             List((neu._1, SimpleType(foundType), value, foundType, neu._1)) ::: rec(lst, neu._2)
//           case StringType(len) =>
//             val neu = if (name == "") fun(s"char_field", map) else fun(s"$name", map)
//             List((neu._1, StringType(len), value, "char", s"${neu._1}[${len}]")) ::: rec(lst, neu._2)
//         }
//       }
//       lst match {
//         case x::xs => x match {
//           case Attribute(name) => proc(name, x, xs)
//           case _ => proc("", x, xs)
//         }
//         case Nil => List[(String, DataType, String, String, String)]()
//       }
//     }
//     rec(lst, Map[String, Int]())
//   }

//   def genAssign(ty: DataType, name: String, value: String, new_struct: String) = {
//     val dest = s"${new_struct}.${name}"
//     ty match {
//       case SimpleType(_) => s"$dest = ${value};"
//       case StringType(_) => s"std::strcpy($dest, ${value});"
//     }
//   }

//   def newTag(ref: Ref): String = {
//     val newValue = ref.counter + 1
//     ref.fun(newValue)
//     s"query${ref.relNum}_${ref.counter}"
//   }

//   def pushOpFilter(
//     meta: RelationMetaData,
//     expr: Expr,
//     iterName: String,
//     pushOpName: String,
//     projection: String
//   ): (String, String, String ) = {
//     def findForeignVars(expr: Expr): List[(String, DataType)] = {
//       expr match {
//         case x: BinOp => findForeignVars(x.left) ::: findForeignVars(x.right)
//         case Not(x) => findForeignVars(x)
//         case OutsideVar(name, varType) => List((name, varType))
//         case _ => Nil
//       }
//     }
//     def fieldDef(name: String, dataType: DataType) = {
//       dataType match {
//         case SimpleType(x) => s"$x $name;"
//         case StringType(x) => s"char ${name}[$x];"
//       }
//     }
//     def varCpy(name: String, dataType: DataType) = {
//       val dest = s"${pushOpName}.${name}"
//       dataType match {
//         case SimpleType(_) => s"$dest = ${name};"
//         case StringType(_) => s"std::strcpy($dest, ${name});"
//       }
//     }
//     val foreignVars = findForeignVars(expr)
//     val oldListName = s"${meta.name}"
//     val listType = s"${meta.typeName}"
//     val newListName = s"${oldListName}_filter"
//     if (typeLookup(expr, meta) != SimpleType("bool"))
//       throw new Error("WHERE condition must be of type bool")
//     val test = s"""
// // filter
// if (${condTrans(expr, meta, iterName)}) {
//   ${projection}
// }
// """
//     val fieldsDef = foreignVars.foldLeft("") ( (acc, x) => acc + "    " + fieldDef(x._1, x._2) + "\n" )
//     val varsCpy = foreignVars.foldLeft("") ( (acc, x) => acc + varCpy(x._1, x._2) + "\n" )
//     (test, fieldsDef, varsCpy)
//   }

//   def pushOpProj(meta: RelationMetaData, exprList: List[Expr]):
//       (String, String, RelationMetaData) = {
//     val oldIterName = meta.name
//     val newIterName = s"${oldIterName}_proj"
//     val newTypeName = s"${meta.typeName}_proj"
//     val mainList = genMap(exprList, oldIterName, meta)
//     val struct = s"struct ${newTypeName} {\n" + mainList.foldLeft("") ((acc, x) => acc + "  " + x._4 + " " + x._5 +";\n" )+ "}MACRO_PACKED;\n"
//     val code = s"""
//   ${newTypeName} ${newIterName}{};
// ${mainList.foldLeft("") ((acc, x) => acc + "  " + genAssign(x._2, x._1, x._3, newIterName) + "\n").dropRight(1)}
//   vec.push_back(${newIterName});
// """
//     // println("HELELELELEL: \n "+code)
//     val newMap = (mainList.map{ case x => (x._1 -> x._2) }).toMap
//     (struct, code, RelationMetaData(newMap, newIterName, newTypeName))
//   }

//   def rangeScan(
//     meta: TableMetaData,
//     from: List[(DataType, RangeVal)],
//     to: List[(DataType, RangeVal)],
//     ref: Ref,
//     filterExpr: Option[Expr],
//     projList: Option[List[Expr]]
//   ) = {
//     def conv(
//       value: RangeVal,
//       indexType: DataType,
//       meta: TableMetaData,
//       struct: String,
//       field: String): String = {
//       val curName = struct + "." + field
//       indexType match {
//         case SimpleType(typeName) => value match {
//           case ConstVal(expr) =>
//             val exp = condTrans(expr, RelationMetaData(meta.attributes, "", ""), "")
//             s"$curName = $exp;"
//           case ZeroVal() => s"$curName = std::numeric_limits<${typeName}>::min();"
//           case MaxVal()  => s"$curName = std::numeric_limits<${typeName}>::max();"
//         }
//         case StringType(len) => value match {
//           case ConstVal(expr) =>
//             val str = condTrans(expr, RelationMetaData(meta.attributes, "", ""), "")
//             // TODO: Buffer overflow?
//             // Include <cstring>
//             s"std::strcpy($curName, $str);"
//           case ZeroVal() => ""
//           case MaxVal() =>
//             s"std::fill_n($curName, $len, std::numeric_limits<unsigned char>::max());"
//         }
//       }
//     }
//     val relName = s"${meta.relName}"
//     val listName = s"${newTag(ref)}_${relName}"
//     val push = s"${listName}_push"
//     val pushType = s"${push}_type"
//     val index = s"${listName}_index"
//     val fromString = s"${listName}_from_string"
//     val toString = s"${listName}_to_string"
//     val fromKey = s"${listName}_from_key"
//     val toKey = s"${listName}_to_key"
//     val keyType = s"${relName}_key_type"
//     val valType = s"${relName}_val_type"
//     val iterName = "i"
//     val relMeta = RelationMetaData(meta.attributes, iterName, listName + "_val_type")
//     val (projectStruct, projectCode, projMeta) = projList match {
//       case Some(lst) => pushOpProj(relMeta, lst)
//       case None => ("", s"vec.push_back($iterName);\n",
//         RelationMetaData(meta.attributes, iterName, valType))
//     }
//     val (filterTest, filterFieldDef, filterVarCpy) = filterExpr match {
//       case Some(expr) => pushOpFilter(projMeta, expr, iterName, push, projectCode)
//       case None => (projectCode, "", "")
//     }
//     val code =s"""
// ${projectStruct}

// class ${pushType}: public reactdb::abstract_push_op {
//   public:
// ${filterFieldDef}
//     std::vector<${projMeta.typeName}> vec{};
//     bool invoke(const char *keyp, size_t keylen, const std::string &value) override {
//       ${valType} ${iterName}{};
//       reactdb::utility::decode_safe(value, ${iterName});
//       ${filterTest}
//       return true;
//     }
// };

// auto ${index} = get_index("${relName}");
// ${pushType} ${push};
// ${filterVarCpy}
// ${keyType} ${fromKey}{};
// ${((from zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, fromKey, y) + "\n"}) }
// ${keyType} ${toKey}{};
// ${((to zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, toKey, y) + "\n"}) }
// const auto& ${fromString} = reactdb::utility::encode_safe(${fromKey});
// const auto& ${toString} = reactdb::utility::encode_safe(${toKey});
// ${index}->range_scan(${fromString}, &${toString}, ${push});
// std::vector<${projMeta.typeName}> ${listName} = ${push}.vec;

// """
//     ("\n//range scan" + code, RelationMetaData(projMeta.attributes, listName, projMeta.typeName))
//   }

//   val meta = TableMetaData("saving", List("account_id"),
//     Map("account_id" -> SimpleType("int"),
//       "user_name" -> StringType(255),
//       "balance" -> SimpleType("int")))
//   val exp = Less(Attribute("account_id"), Plus(OutsideVar("outside_id", SimpleType("int")), OutsideVar("some_id", SimpleType("int"))))
//   val relMeta = RelationMetaData(
//     Map("account_id" -> SimpleType("int"),
//     "user_name" -> StringType(255),
//       "balance" -> SimpleType("int")),
//     "someName", "someType"
//   )
//   // def pushOpFilter(meta: RelationMetaData, expr: Expr, iterName: String, pushOpName: String):
//   // val (one, two, three) = pushOpFilter(
//   //   RelationMetaData(meta.attributes, "some_name", "some_type"),
//   //   exp,
//   //   "iter",
//   //   "my_push_op")
//   // println(s"$one\n$two$three")
//   // val foo = pushOpProj(relMeta, List(Attribute("user_name")))
//   // println(foo._1 + "\n" + foo._2 + "\n" + foo._3)
//   var tagNumber = 0
//   val queryNum = 0
//   val ref = Ref(queryNum, tagNumber, (tagNumber = _))
//   val bar = rangeScan(
//     meta,
//     List((SimpleType("int"), ZeroVal())),
//     List((SimpleType("int"), MaxVal())),
//     ref,
//     Some(Less(Attribute("account_id"), Const("2", SimpleType("int")))),
//     // None,
//     Some(List(Attribute("account_id"))))
//     // None)
//   println(bar)
// }
