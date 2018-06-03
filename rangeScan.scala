import CodeGenUtils._

object RangeScanGen {
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
        // val value = condTrans(expr, Map("" -> meta), structName)
        val value = condTrans(expr, Map("" -> meta), "")
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
          case DerefExp(alias, name) => proc((alias + "_" + name), x, xs)
          case Attribute(name) => throw new Error("An attribute reference has not received an alias!")
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
    val newMap = (mainList.map{ case x => (x._1 -> x._2) }).toMap
    // (struct, code, RelationMetaData(newMap, newIterName, newTypeName))
    (struct, code, RelationMetaData(newMap, oldIterName, newTypeName))
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
          // case ZeroVal() => s"$curName = std::numeric_limits<${typeName}>::min();"
          case ZeroVal() => s""
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
    // val keyType = s"${listName}_key_type"
    val keyType = s"${meta.relName}_key_type"
    val valType = s"${listName}_val_type"
    val iterName = "i"
    val relMeta = RelationMetaData(meta.attributes, iterName, listName + "_val_type")
    val (projectStruct, projectCode, projMeta) = projList match {
      case Nil => ("", s"vec.push_back($iterName);\n",
        RelationMetaData(meta.attributes, iterName, valType))
      case lst => pushOpProj(relMeta, lst)
    }
    val (filterTest, filterFieldDef, filterVarCpy) = filterExpr match {
      // case Some(expr) => pushOpFilter(projMeta, expr, iterName, push, projectCode)
      case Some(expr) => pushOpFilter(RelationMetaData(meta.attributes, iterName, valType), expr, iterName, push, projectCode)
      case None => (projectCode, "", "")
    }
    val toKeyAddition = if (to.exists{ case x => !x._1.isInstanceOf[MaxVal] })
      s"""
if (${toString}[${toString}.size()-1] != (char)std::numeric_limits<unsigned char>::max()) {
  ${toString}[${toString}.size()-1]++;
}"""
      else ""

    val code =s"""
${newValueStruct(meta, valType)}
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
${((from zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, fromKey, y._1) + "\n"}) }
${keyType} ${toKey}{};
${((to zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, toKey, y._1) + "\n"}) }
const auto& ${fromString} = reactdb::utility::encode_safe(${fromKey});
auto ${toString} = reactdb::utility::encode_safe(${toKey});
${toKeyAddition}
${index}->range_scan(${fromString}, &${toString}, ${push});
std::vector<${projMeta.typeName}> ${listName} = ${push}.vec;

"""
    ("\n//range scan" + code, RelationMetaData(projMeta.attributes, listName, projMeta.typeName))
  }
}
