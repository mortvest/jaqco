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

sealed trait RelAlg
case class Rel(relName: String, alias: String) extends RelAlg
case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
case class Selection(cond: Expr, from: RelAlg) extends RelAlg
case class Cross(left: RelAlg, right: RelAlg) extends RelAlg

case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, DataType])

case class RelationMetaData(attributes: Map[String, DataType], name: String, typeName: String)
//hack to introduce call by reference
case class Ref(relNum: Int, counter: Int, fun: (Int => Unit))
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

object a extends App {
  var tagNumber = 0
  val queryNum = 0
  def genCode(physical: Physical, relMeta: RelationMetaData): (String, RelationMetaData) = {
    val ref = Ref(queryNum, tagNumber, (tagNumber = _))
    physical match {
      case RangeScan(meta, from, to, alias, projList, selectExpr) =>
        rangeScan(meta,from, to, ref, selectExpr, projList)
        // ("// index lookup", RelationMetaData(Map(), "", ""))
      case x:IndexLookup => ("// index lookup", RelationMetaData(Map(), "", ""))
      // case RangeScan(meta, from, to, alias, projList, expr) =>
      // case IndexLookup(meta, map) =>
    }
  }
  def typeLookup(expr: Expr, metaMap: Map[String, RelationMetaData]): DataType = {
    def checkBin(left: Expr, right: Expr) = {
      val l = typeLookup(left, metaMap)
      val r = typeLookup(right, metaMap)
      if (l.isInstanceOf[StringType] || r.isInstanceOf[StringType])
        throw new Error(s"Binary operation in not supported for strings")
      else if (l == r)
        l
      else throw new Error(s"Operands $l and $r must be of the same type")
    }
    def checkBool(left: Expr, right: Expr, opName: String) = {
      val l = typeLookup(left, metaMap)
      val r = typeLookup(right, metaMap)
      if (l == SimpleType("bool") && r == SimpleType("bool")) l
      else throw new Error(s"Operands of ${opName} must be boolean")
    }
    def checkComparison(left: Expr, right: Expr) = {
      val l = typeLookup(left, metaMap)
      val r = typeLookup(right, metaMap)
      if (l != r) throw new Error("Comparison is only allowed between values of the same type")
      else SimpleType("bool")
    }
    expr match {
      case DerefExp(alias, name) =>
        val getType = (meta: RelationMetaData) => meta.attributes get (alias + "_" + name) match {
          case None => throw new Error(s"""Can not determine the type of variable \"${name}\" """)
          case Some(varType) => varType
        }
        metaMap get "" match {
          case None =>
            metaMap get alias match {
              case None => throw new Error(s"""Alias \"${alias}\" could not be found""")
              case Some(meta) => getType(meta)
            }
          case Some(meta) => println(alias + "\n" + meta); getType(meta)
        }
      case OutsideVar(name, varType) => varType
      case Const(value, valType) => valType
      case And(left, right) => checkBool(left, right, "AND")
      case Or(left, right) => checkBool(left, right, "OR")
      case x: RangeOp => checkComparison(x.left, x.right)
      case Equals(left, right) => checkComparison(left, right)
      case x: BinOp => checkBin(x.left, x.right)
      case Not(expr) =>
        val e = typeLookup(expr, metaMap)
        if (e != SimpleType("bool")) throw new Error(s"Argument of NOT must be boolean")
        else e
      case _ => throw new Error(s"Can not determine the type of expression")
    }
  }
  def condTrans(expr: Expr, meta: Map[String, RelationMetaData], postFix: String): String = {
    def comparison(left: Expr, right: Expr, simpleOp: String, stringOp: String ) = {
      typeLookup(left, meta) match {
        case SimpleType(_) => "("  + trans(left) + simpleOp + trans(right) + ")"
        case StringType(_) => s"(strcmp(${trans(left)}, ${trans(right)}) ${stringOp})"
      }
    }
    def trans(expr: Expr): String = {
      expr match {
        case DerefExp(alias, name) => meta get "" match {
          case None =>
            meta get alias match {
              case None => throw new Error(s"""Alias \"${alias}\" could not be found""")
              // case Some(relMeta) => relMeta.name + "." + alias + "_" + name
              case Some(relMeta) => relMeta.name + postFix + "." + alias + "_" + name
            }
          case Some(relMeta) => relMeta.name + postFix + "." + alias + "_" + name
        }
        case Attribute(name)      => throw new Error(s"""Attribute \"${name}\" did not receive an alias""")
        case OutsideVar(name, _)  => name
        case Equals(left, right)  => comparison(left, right, " == ", "== 0")
        case Less(left, right)    => comparison(left, right, " < ", "< 0")
        case Leq(left, right)     => comparison(left, right, " <= ", "<= 0")
        case Greater(left, right) => comparison(left, right, " > ", "> 0")
        case Geq(left, right)     => comparison(left, right, " >= ", ">= 0")
        case Plus(left, right)    => "("  + trans(left) + " + "  + trans(right) + ")"
        case Minus(left, right)   => "("  + trans(left) + " - "  + trans(right) + ")"
        case And(left, right)     => "("  + trans(left) + " && " + trans(right) + ")"
        case Or(left, right)      => "("  + trans(left) + " || " + trans(right) + ")"
        case Not(value)           => "!(" + trans(value) + ")"
        case Const(x, valType)    => valType match {
          case SimpleType(_) => x
          case StringType(_) => "\"" + x + "\""
        }
        case _  => throw new Error("Expression translation failed")
      }
    }
    trans(expr)
  }
  def thetaJoin(cond: Option[Expr], opList: List[Physical]) = {
    def nestedLoopJoin(cond: Option[Expr], metaMap: Map[String, RelationMetaData]):
        (String, RelationMetaData) = {
      def mergeMeta(lst: Map[String, RelationMetaData], newName: String, newType: String) = {
        val newAtts = lst.foldLeft(Map[String, DataType]()) ( (acc, x) => acc ++ x._2.attributes )
        RelationMetaData(newAtts, newName, newType)
      }
      def genStructLine(valType: DataType, name: String) = {
        valType match {
          case SimpleType(tp) => s"$tp $name"
          case StringType(len) => s"char $name[$len]"
        }
      }
      val joinName = "query0_2_join"
      val joinType = s"${joinName}_type"
      val newMeta = mergeMeta(metaMap, joinName, joinType)
      val struct = s"struct ${joinType} {\n" + newMeta.attributes.foldLeft("") ((acc, x) =>
          acc + "  " + genStructLine(x._2, x._1) + ";\n" )+ "}MACRO_PACKED;\n"
      val loop = (inside: String) =>
      (metaMap.foldLeft ("") ( (acc, x) => acc + s"for (auto ${x._2.name}_i : ${x._2.name}){\n" )) + inside + metaMap.foldLeft ("") ( (acc, x) => acc + "}\n" )
      val vectorCreation = s"std::vector<${joinType}> ${joinName} {};\n"
      val joinCond = (copy: String) => cond match {
        case None => s"$copy\n"
        case Some(expr) =>
          if (typeLookup(expr, metaMap) != SimpleType("bool"))
            throw new Error("WHERE condition must be of type bool")
          else
            s"  if (${condTrans(expr, metaMap, "_i")}) {\n${copy}\n}\n"
      }
      def varCpyFun(name: String, dataType: DataType, from: String) = {
        val dest = s"${joinName}_i.${name}"
        dataType match {
          case SimpleType(_) => s"$dest = $from.$name"
          case StringType(_) => s"std::strcpy($dest, $from.$name)"
        }
      }
      val varCpy = s"""    ${joinType} ${joinName}_i {};
${metaMap.foldLeft ("")( (acc, x) => acc + (x._2.attributes.foldLeft ("") ((acc, y) => acc + "    " + varCpyFun(y._1, y._2, x._2.name + "_i") + ";\n")) )}
    ${joinName}.push_back(${joinName}_i);"""
      val code = s"""
$struct
$vectorCreation
${loop(joinCond(varCpy))}
"""
      (code, newMeta)
    }
    def getAlias(op: Physical): String = {
      op match {
        case x: IndexLookup => x.alias
        case x: RangeScan => x.alias
        case x => throw new Error(s"Unexpected physical operator inside the join list")
      }
    }
    val (opCode, metaLst) =
      (opList.map {
        case x =>
          val (code, relMeta) = genCode(x, RelationMetaData(Map(), "", ""))
          (code, (getAlias(x), relMeta))
      }).unzip
    val materialization = opCode.foldLeft("") ((acc, x) => acc + x)
    val (fullCode, newMeta)  = nestedLoopJoin(cond, metaLst.toMap)
    (materialization + fullCode, newMeta)
  }

  def onePassProj(meta: RelationMetaData, exprList: List[Expr]) = {
    def genMap(lst: List[Expr], structName: String): List[(String, DataType, String, String, String)] = {
      def fun(value: String, map: Map[String, Int]): (String, Map[String, Int]) = {
        (map get value) match {
          case None => (value, map + (value -> 1))
          case Some(x) => (value + x.toString, map + (value -> (x + 1)))
        }
      }
      def rec(lst: List[Expr], map: Map[String, Int]): List[(String, DataType, String, String, String)] = {
        def proc(name: String, expr: Expr, lst: List[Expr]) = {
          val value = condTrans(expr, Map("" -> meta), structName)
          typeLookup(expr,Map("" -> meta)) match {
            case SimpleType(foundType) =>
              val neu = if (name == "") fun(s"${foundType}_field", map) else fun(s"$name", map)
              List((neu._1, SimpleType(foundType), value, foundType, neu._1)) ::: rec(lst, neu._2)
            case StringType(len) =>
              val neu = if (name == "") fun(s"char_field", map) else fun(s"$name", map)
              List((neu._1, StringType(len), value, "char", s"${neu._1}[${len}]")) ::: rec(lst, neu._2)
          }
        }
        lst match {
          case x::xs => x match {
            case Attribute(name) => proc(name, x, xs)
            case _ => proc("", x, xs)
          }
          case Nil => List[(String, DataType, String, String, String)]()
        }
      }
      rec(lst, Map[String, Int]())
    }
    val oldListName = meta.name
    val newListName = s"${oldListName}_proj"
    val oldTypeName = oldListName + "_type"
    val newTypeName = newListName + "_type"
    val itr1 = "i"
    val itr2 = "old_i"
    val itr3 = "new_i"
    val mainList = genMap(exprList, itr2)
    val struct = s"struct ${newTypeName} {\n" + mainList.foldLeft("") ((acc, x) => acc + "  " + x._4 + " " + x._5 +";\n" )+ "}MACRO_PACKED;\n"
    val code = s"""
std::vector<${newTypeName}> ${newListName};
for (auto &${itr2} : ${oldListName}) {
  ${newTypeName} $itr3{};
${mainList.foldLeft("") ((acc, x) => acc + "  " + genAssign(x._2, x._1, x._3, itr3) + "\n").dropRight(1)}
  ${newListName}.push_back($itr3);
}
"""
    val newMap = (mainList.map{ case x => (x._1 -> x._2) }).toMap
    ("//projection\n" + struct + code, RelationMetaData(newMap, s"$newListName", s"$newTypeName"))
  }

  def genAssign(ty: DataType, name: String, value: String, new_struct: String) = {
    val dest = s"${new_struct}.${name}"
    ty match {
      case SimpleType(_) => s"$dest = ${value};"
      case StringType(_) => s"std::strcpy($dest, ${value});"
    }
  }

  def newTag(ref: Ref): String = {
    val newValue = ref.counter + 1
    ref.fun(newValue)
    s"query${ref.relNum}_${ref.counter}"
  }

  def indexLookup(meta: TableMetaData, map: Map[String, Expr], ref: Ref) = {
    def genAssign(name: String, value: String, new_struct: String) = {
      val dest = s"${new_struct}.${name}"
        (meta.attributes get name).get match {
          case SimpleType(_) => s"${dest} = ${value};"
          case StringType(_) => s"std::strcpy($dest, ${value});"
        }
    }
    val relName  = s"${meta.relName}"
    val listName = s"${newTag(ref)}_${relName}"
    val keyStruct = s"${listName}_key"
    val keyType = s"${relName}_key_type"
    val valueString = s"${listName}_value_string"
    // val keyStructCreate = map.foldLeft("") ( (acc, ex) =>
    //   acc + keyStruct + "." + ex._1 + " = " + condTrans(ex._2, RelationMetaData(Map(),"",""), "") + ";\n")
    val keyStructCreate = map.foldLeft("") ( (acc, ex) =>
      acc + genAssign(ex._1 ,condTrans(ex._2, Map("" -> RelationMetaData(Map(),"","")), ""), keyStruct) + ";\n")
    val valStruct = s"${relName}_value"
    val valType = s"${relName}_type"
    val index = s"${listName}_index"
    val code = s"""
//index lookup
auto $index = get_index("$relName");
std::string ${valueString};
$keyType $keyStruct {};
$keyStructCreate
std::vector<${valType}> $listName;

if ($index->get(utility::encode_safe($keyStruct), ${valueString})) {
  $valType $valStruct {};
  utility::decode_safe(${valueString}, ${valStruct});
  ${listName}.push_back(${valStruct});
}
"""
    (code, RelationMetaData(meta.attributes, listName, valType))
  }


  def rangeScan(
    meta: TableMetaData,
    from: List[(DataType, RangeVal)],
    to: List[(DataType, RangeVal)],
    ref: Ref,
    filterExpr: Option[Expr],
    projList: List[Expr]
  ) = {
  def genMap(lst: List[Expr], structName: String, meta: RelationMetaData): List[(String, DataType, String, String, String)] = {
    def fun(value: String, map: Map[String, Int]): (String, Map[String, Int]) = {
      (map get value) match {
        case None => (value, map + (value -> 1))
        case Some(x) => (value + x.toString, map + (value -> (x + 1)))
      }
    }
    def rec(lst: List[Expr], map: Map[String, Int]): List[(String, DataType, String, String, String)] = {
      def proc(name: String, expr: Expr, lst: List[Expr]) = {
        val value = condTrans(expr, Map("" -> meta), structName)
        typeLookup(expr, Map("" -> meta)) match {
          case SimpleType(foundType) =>
            val neu = if (name == "") fun(s"${foundType}_field", map) else fun(s"$name", map)
            List((neu._1, SimpleType(foundType), value, foundType, neu._1)) ::: rec(lst, neu._2)
          case StringType(len) =>
            val neu = if (name == "") fun(s"char_field", map) else fun(s"$name", map)
            List((neu._1, StringType(len), value, "char", s"${neu._1}[${len}]")) ::: rec(lst, neu._2)
        }
      }
      lst match {
        case x::xs => x match {
          case Attribute(name) => proc(name, x, xs)
          case _ => proc("", x, xs)
        }
        case Nil => List[(String, DataType, String, String, String)]()
      }
    }
    rec(lst, Map[String, Int]())
  }

  def pushOpFilter(
    meta: RelationMetaData,
    expr: Expr,
    iterName: String,
    pushOpName: String,
    projection: String
  ): (String, String, String ) = {
    def findForeignVars(expr: Expr): List[(String, DataType)] = {
      expr match {
        case x: BinOp => findForeignVars(x.left) ::: findForeignVars(x.right)
        case Not(x) => findForeignVars(x)
        case OutsideVar(name, varType) => List((name, varType))
        case _ => Nil
      }
    }
    def fieldDef(name: String, dataType: DataType) = {
      dataType match {
        case SimpleType(x) => s"$x $name;"
        case StringType(x) => s"char ${name}[$x];"
      }
    }
    def varCpy(name: String, dataType: DataType) = {
      val dest = s"${pushOpName}.${name}"
      dataType match {
        case SimpleType(_) => s"$dest = ${name};"
        case StringType(_) => s"std::strcpy($dest, ${name});"
      }
    }
    val foreignVars = findForeignVars(expr)
    val oldListName = s"${meta.name}"
    val listType = s"${meta.typeName}"
    val newListName = s"${oldListName}_filter"
    if (typeLookup(expr, Map("" -> meta)) != SimpleType("bool"))
      throw new Error("WHERE condition must be of type bool")
    val test = s"""
// filter
if (${condTrans(expr, Map("" -> meta), "")}) {
  ${projection}
}
"""
    val fieldsDef = foreignVars.foldLeft("") ( (acc, x) => acc + "    " + fieldDef(x._1, x._2) + "\n" )
    val varsCpy = foreignVars.foldLeft("") ( (acc, x) => acc + varCpy(x._1, x._2) + "\n" )
    (test, fieldsDef, varsCpy)
  }

  def pushOpProj(meta: RelationMetaData, exprList: List[Expr]):
      (String, String, RelationMetaData) = {
    val oldIterName = meta.name
    val newIterName = s"${oldIterName}_proj"
    val newTypeName = s"${meta.typeName}_proj"
    val mainList = genMap(exprList, oldIterName, meta)
    val struct = s"struct ${newTypeName} {\n" + mainList.foldLeft("") ((acc, x) => acc + "  " + x._4 + " " + x._5 +";\n" )+ "}MACRO_PACKED;\n"
    val code = s"""
  ${newTypeName} ${newIterName}{};
${mainList.foldLeft("") ((acc, x) => acc + "  " + genAssign(x._2, x._1, x._3, newIterName) + "\n").dropRight(1)}
  vec.push_back(${newIterName});
"""
    // println("HELELELELEL: \n "+code)
    val newMap = (mainList.map{ case x => (x._1 -> x._2) }).toMap
    (struct, code, RelationMetaData(newMap, newIterName, newTypeName))
  }

    def conv(
      value: RangeVal,
      indexType: DataType,
      meta: TableMetaData,
      struct: String,
      field: String): String = {
      val curName = struct + "." + field
      indexType match {
        case SimpleType(typeName) => value match {
          case ConstVal(expr) =>
            val exp = condTrans(expr, Map("" -> RelationMetaData(meta.attributes, "", "")), "")
            s"$curName = $exp;"
          case ZeroVal() => s"$curName = std::numeric_limits<${typeName}>::min();"
          case MaxVal()  => s"$curName = std::numeric_limits<${typeName}>::max();"
        }
        case StringType(len) => value match {
          case ConstVal(expr) =>
            val str = condTrans(expr, Map("" -> RelationMetaData(meta.attributes, "", "")), "")
            // TODO: Buffer overflow?
            // Include <cstring>
            s"std::strcpy($curName, $str);"
          case ZeroVal() => ""
          case MaxVal() =>
            s"std::fill_n($curName, $len, std::numeric_limits<unsigned char>::max());"
        }
      }
    }
    val relName = s"${meta.relName}"
    val listName = s"${newTag(ref)}_${relName}"
    val push = s"${listName}_push"
    val pushType = s"${push}_type"
    val index = s"${listName}_index"
    val fromString = s"${listName}_from_string"
    val toString = s"${listName}_to_string"
    val fromKey = s"${listName}_from_key"
    val toKey = s"${listName}_to_key"
    val keyType = s"${relName}_key_type"
    val valType = s"${relName}_val_type"
    val iterName = "i"
    val relMeta = RelationMetaData(meta.attributes, iterName, listName + "_val_type")
    val (projectStruct, projectCode, projMeta) = projList match {
      case Nil => ("", s"vec.push_back($iterName);\n",
        RelationMetaData(meta.attributes, iterName, valType))
      case lst => pushOpProj(relMeta, lst)
    }
    val (filterTest, filterFieldDef, filterVarCpy) = filterExpr match {
      case Some(expr) => pushOpFilter(projMeta, expr, iterName, push, projectCode)
      case None => (projectCode, "", "")
    }
    val code =s"""
${projectStruct}

class ${pushType}: public reactdb::abstract_push_op {
  public:
${filterFieldDef}
    std::vector<${projMeta.typeName}> vec{};
    bool invoke(const char *keyp, size_t keylen, const std::string &value) override {
      ${valType} ${iterName}{};
      reactdb::utility::decode_safe(value, ${iterName});
      ${filterTest}
      return true;
    }
};

auto ${index} = get_index("${relName}");
${pushType} ${push};
${filterVarCpy}
${keyType} ${fromKey}{};
${((from zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, fromKey, y) + "\n"}) }
${keyType} ${toKey}{};
${((to zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, toKey, y) + "\n"}) }
const auto& ${fromString} = reactdb::utility::encode_safe(${fromKey});
const auto& ${toString} = reactdb::utility::encode_safe(${toKey});
${index}->range_scan(${fromString}, &${toString}, ${push});
std::vector<${projMeta.typeName}> ${listName} = ${push}.vec;

"""
    ("\n//range scan" + code, RelationMetaData(projMeta.attributes, listName, projMeta.typeName))
  }
  // val meta = Map[String, RelationMetaData](
  //   "saving" ->
  //     RelationMetaData(
  //       Map("saving_user_name" -> StringType(255),
  //         "saving_balance" -> SimpleType("int")),
  //       "query0_0_saving",
  //       "saving_value_type"
  //     ),
  //   "other_table" ->
  //     RelationMetaData(
  //       Map("other_table_user_name" -> StringType(255),
  //         "other_table_balance" -> SimpleType("int")),
  //       "query0_1_other_table",
  //       "other_table_value_type"
  //     ),
  // )
  val foo = Less(DerefExp("A", "balance"), DerefExp("B", "balance"))
  // println(nestedLoopJoin(Some(foo), meta))
  // // println(thetaJoin(None, meta))
  // val bar = List(DerefExp("saving", "balance"))
  // val coo = RelationMetaData(
  //   Map("saving_user_name" -> StringType(255),
  //     "saving_balance" -> SimpleType("int")),
  //   "query0_0_saving",
  //   "saving_value_type"
  // )
  // println(onePassProj(coo, bar))
  val bar = Less(DerefExp("A", "balance"), Const("2", SimpleType("int")))
  val bar1 = Less(DerefExp("B", "balance"), Const("2", SimpleType("int")))
  val tMetaA = TableMetaData("saving", List("A_user_name"),
    Map("A_user_name" -> StringType(255),
      "A_balance" -> SimpleType("int")))

  val tMetaB = TableMetaData("saving", List("B_user_name"),
    Map("B_user_name" -> StringType(255),
      "B_balance" -> SimpleType("int")))

  val rsA = RangeScan(tMetaA, List((StringType(5), ZeroVal())), List((StringType(5), MaxVal())), "A",Nil, Some(bar))

  val rsB = RangeScan(tMetaB, List((StringType(5), ZeroVal())), List((StringType(5), MaxVal())), "B",Nil, Some(bar1))

  println(thetaJoin(Some(bar), List(rsA, rsB)))
  // println(
  //   rangeScan(
  //     tMeta,
  //     List((StringType(5), ZeroVal())), List((StringType(5), MaxVal())), ref, Some(bar), Nil))
}
