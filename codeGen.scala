case class RelationMetaData(attributes: Map[String, DataType], name: String, typeName: String)
//hack to introduce call by reference
case class Ref(relNum: Int, counter: Int, fun: (Int => Unit))

object CodeGeneration {
  def apply(physical: Physical, varName: String, queryNum: Int) = {
    var tagNumber = 0
    def genCode(physical: Physical, relMeta: RelationMetaData): (String, RelationMetaData) = {
      val ref = Ref(queryNum, tagNumber, (tagNumber = _))
      physical match {
        case RangeScan(meta, from, to) => rangeScan(meta, from, to, ref)
        case IndexLookup(meta, map) => indexLookup(meta, map, ref)
        case Filter(expr, from) =>
          val x = genCode(from, relMeta)
          val y = filter(x._2, expr)
          (x._1 + y._1, y._2)
        case OnePassProj(exprList, from) =>
          val x = genCode(from, relMeta)
          val y = onePassProj(x._2, exprList)
          (x._1 + y._1, y._2)
      }
    }
    val nameTags = scala.collection.mutable.Set
    val tag = "////////////////////////////////// GENERATED BY JAQCO ///////////////////////////////////"
    genCode(physical, RelationMetaData(Map(), "", "")) match {
      case (code, relMap) =>
        tag + code + s"auto $varName = *(std::move(&${relMap.name}));\n" + tag
    }
  }

  def newTag(ref: Ref): String = {
    val newValue = ref.counter + 1
    ref.fun(newValue)
    s"query${ref.relNum}_${ref.counter}"
  }

  def filter(meta: RelationMetaData, expr: Expr): (String, RelationMetaData) = {
    val oldListName = s"${meta.name}"
    val listType = s"${meta.typeName}"
    val newListName = s"${oldListName}_filter"
    val iterator = s"i"
    val code = s"""
// filter
std::vector<${listType}> ${newListName};
for (const auto &${iterator} : ${oldListName}) {
  if (${condTrans(expr, meta, iterator)}) {
    ${newListName}.push_back(${iterator});
  }
}

"""
    (code, RelationMetaData(meta.attributes, newListName, meta.typeName))
  }

  def rangeScan(meta: TableMetaData, from: List[(DataType, RangeVal)], to: List[(DataType, RangeVal)],
    ref: Ref) = {
    def conv(value: RangeVal, indexType: DataType, meta: TableMetaData, struct: String, field: String): String = {
      val curName = struct + "." + field
      indexType match {
        case SimpleType(typeName) => value match {
          case ConstVal(expr) =>
            val exp = condTrans(expr, RelationMetaData(meta.attributes, "", ""), "")
            s"$curName = $exp;"
          case ZeroVal() => s"$curName = std::numeric_limits<${typeName}>::min();"
          case MaxVal()  => s"$curName = std::numeric_limits<${typeName}>::max();"
        }
        case StringType(len) => value match {
          case ConstVal(expr) =>
            val str = condTrans(expr, RelationMetaData(meta.attributes, "", ""), "")
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
    val pushType = s"${relName}_push_type"
    val index = s"${listName}_index"
    val fromString = s"${listName}_from_string"
    val toString = s"${listName}_to_string"
    val fromKey = s"${listName}_from_key"
    val toKey = s"${listName}_to_key"
    val keyType = s"${relName}_key_type"
    val valType = s"${relName}_val_type"
    val code =s"""
auto ${index} = get_index("${relName}");
${pushType} ${push};

${keyType} ${fromKey}{};
${((from zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, fromKey, y) + "\n"}) }
${keyType} ${toKey}{};
${((to zip meta.indexParts).foldLeft(""){ case (acc, (x, y)) => acc + conv(x._2, x._1, meta, toKey, y) + "\n"}) }
const auto& ${fromString} = reactdb::utility::encode_safe(${fromKey});
const auto& ${toString} = reactdb::utility::encode_safe(${toKey});
${index}->range_scan(&${fromString}, &${toString}, ${push});
auto ${listName} = std::move(${push}.vec);

"""
    ("\n//range scan" + code, RelationMetaData(meta.attributes, listName, valType))
  }

  def condTrans(expr: Expr, relMeta: RelationMetaData, structName: String): String = {
    def trans(expr: Expr): String = {
      expr match {
        case Attribute(attName) => val x = relMeta.attributes.keys.toList.contains(attName)
          if (x) structName + "." + attName else attName
        case Less(left, right)    => "("  + trans(left) + " < "  + trans(right) + ")"
        case Leq(left, right)     => "("  + trans(left) + " <= " + trans(right) + ")"
        case Greater(left, right) => "("  + trans(left) + " > "  + trans(right) + ")"
        case Geq(left, right)     => "("  + trans(left) + " >= " + trans(right) + ")"
        case Equals(left, right)  => "("  + trans(left) + " == " + trans(right) + ")"
        case Plus(left, right)    => "("  + trans(left) + " + "  + trans(right) + ")"
        case Minus(left, right)   => "("  + trans(left) + " - "  + trans(right) + ")"
        case And(left, right)     => "("  + trans(left) + " && " + trans(right) + ")"
        case Or(left, right)      => "("  + trans(left) + " || " + trans(right) + ")"
        case Not(value)           => "!(" + trans(value) + ")"
        case x: Const             =>
          if (x.value.isInstanceOf[String]) "\"" + x.value + "\"" else x.value.toString
        case _                    => throw new Error("Can not translate an expression")
      }
    }
    trans(expr)
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
      acc + genAssign(ex._1 ,condTrans(ex._2, RelationMetaData(Map(),"",""), ""), keyStruct) + ";\n")
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

  def typeLookup(expr: Expr, meta: RelationMetaData): DataType = {
    def checkBin(left: Expr, right: Expr) = {
      val l = typeLookup(left, meta)
      val r = typeLookup(right, meta)
      if (l == r) l else throw new Error(s"Operands of $l and $r must be of the same type")
    }
    def checkBool(left: Expr, right: Expr, opName: String) = {
      val l = typeLookup(left, meta)
      val r = typeLookup(right, meta)
      if (l == SimpleType("bool") && r == SimpleType("bool")) l
      else throw new Error(s"Operands of ${opName} must be boolean")
    }
    expr match {
      case Attribute(attName) => (meta.attributes get attName) match {
        case None => throw new Error(s"Column $attName does not exist")
        case Some(varType) => varType
      }
      case And(left, right) => checkBool(left, right, "AND")
      case Or(left, right) => checkBool(left, right, "OR")
      case x: BinOp => checkBin(x.left, x.right)
      case Not(expr) =>
        val e = typeLookup(expr, meta)
        if (e != SimpleType("bool")) throw new Error(s"Argument of NOT must be type boolean")
        else e
      case LongConst(value)    => SimpleType("long")
      case StringConst(value)  =>
        val len = if (value.size < 255) 255 else 65536
        StringType(len)
      case BooleanConst(value) => SimpleType("bool")
      case CharConst(value)    => SimpleType("char")
      case _ => throw new Error(s"Can not determine the type of expression")
    }
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
          val value = condTrans(expr, meta, structName)
          typeLookup(expr, meta) match {
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
    def genAssign(ty: DataType, name: String, value: String, new_struct: String) = {
      val dest = s"${new_struct}.${name}"
      ty match {
        case SimpleType(_) => s"$dest = ${value};"
        case StringType(_) => s"std::strcpy($dest, ${value});"
      }
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
}
