case class RelationMetaData(attributes: Map[String, String], name: String, typeName: String)

import scala.util.Random

object CodeGeneration {
  def apply(physical: Physical, varName: String) = {
    def genCode(physical: Physical, relMeta: RelationMetaData): (String, RelationMetaData) = {
      physical match {
        case RangeScan(meta, from, to) => rangeScan(meta, from, to)
        case IndexLookup(meta, map) => indexLookup(meta, map)
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
    val tag = "////////////////////////////////// GENERATED BY JAQCO ///////////////////////////////////"
    genCode(physical, RelationMetaData(Map(), "", "")) match {
      case (code, relMap) =>
        tag + code + s"auto $varName = std::move(${relMap.name});\n" + tag
    }
  }

  def genName() = Random.alphanumeric.take(10).mkString

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

  def rangeScan(meta: TableMetaData, from: RangeVal, to: RangeVal) = {
    def conv(value: RangeVal, meta: TableMetaData): String = {
      val index = meta.indexParts.last
      val indexType = (meta.attributes get index).get
      value match {
        case ConstVal(expr) =>
          condTrans(expr, RelationMetaData(meta.attributes, "", ""), "")
        case ZeroVal() => s"std::numeric_limits<$indexType>::min()"
        case MaxVal() => s"std::numeric_limits<$indexType>::max()"
      }
    }
    val relName = s"${meta.relName}"
    val listName = s"${relName}_${genName()}"
    val push = s"${listName}_push"
    val pushType = s"${relName}_type"
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
${keyType} ${fromKey} { ${conv(from, meta)} };
${keyType} ${toKey} { ${conv(to, meta)} };
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

  def indexLookup(meta: TableMetaData, map: Map[String, Expr]) = {
    val relName  = s"${meta.relName}"
    val listName = s"${relName}_${genName()}"
    val keyStruct = s"${listName}_key"
    val keyType = s"${relName}_key_type"
    val valueString = s"${listName}_value_string"
    val keyStructCreate =
      map.foldLeft("") ( (acc, ex) =>
        acc + keyStruct + "." + ex._1 + " = " + condTrans(ex._2,
          RelationMetaData(Map(),"",""), "") + ";\n")
    val valStruct = s"${relName}_value"
    val valType = s"${relName}_type"
    val index = s"${listName}_index"
    val code = s"""
//index lookup
auto $index = get_index("$relName");
std::string ${valueString};
$keyType $keyStruct;
$keyStructCreate
std::vector<${valType}> $listName;

if ($index->get(utility::encode_safe($keyStruct), ${valueString})) {
  $valType $valStruct;
  utility::decode_safe(${valueString}, ${valStruct});
  ${listName}.push_back(${valStruct});
}
"""
    (code, RelationMetaData(meta.attributes, listName, valType))
  }

  def typeLookup(valName: String, meta: RelationMetaData) = {
    // TODO: Should be done properly when operations on multiple relations are added
    (meta.attributes get valName) match {
      case None => throw new Error(s"Type for variable $valName could not be found")
      case Some(varType) => varType
    }
  }
  // TODO: Add support for constants
  def onePassProj(meta: RelationMetaData, exprList: List[Expr]) = {
    // def createRelMeta(exprList: List[Expr], map: Map[String, String]): Map[String, String] = {
    //   exprList match {
    //     case Nil => map
    //     case x :: xs =>
    //       val name = getVarName(x, "")
    //       createRelMeta(xs, map) + (name -> typeLookup(name, meta))
    //   }
    // }
    def getType(value: Expr, meta: RelationMetaData) = {
      value match {
        case Attribute(name) => typeLookup(name, meta)
        case LongConst(x) => "long"
        case StringConst(x) => "std::string"
      }
    }
    def getVarName(expr: Expr, salt: String) = {
      expr match {
        case Attribute(attName) => attName
        case LongConst(value)   => s"long_${genName()}"
        case StringConst(value) => s"string_${genName()}"
        case _ => "" //fail here
      }
    }
    def getValue(expr: Expr, name: String) = {
      expr match {
        case Attribute(attName) => name + "." + attName
        case LongConst(value)   => value.toString
        case StringConst(value) => "\"" + value + "\""
        case _ => "" //fail here
      }
    }
    val oldListName = meta.name
    val newListName = s"${oldListName}_proj"
    val oldTypeName = oldListName + "_type"
    val newTypeName = newListName + "_type"
    val itr1 = "i"
    val itr2 = "old_i"
    val itr3 = "new_i"
    val struct = s"struct ${newTypeName} {\n" +
    exprList.foldLeft("") ( (acc, ex) => acc + getType(ex, meta) + " " + getVarName(ex, newListName) + ";\n" ) + "}MACRO_PACKED;\n"
    // val update = exprList.foldLeft("") ((acc, ex) => acc + (getValue(ex, itr2) + ", ")).dropRight(2)
    val update = exprList.foldLeft("") ((acc, ex) => acc + (condTrans(ex, meta, itr2) + ", ")).dropRight(2)
    val code = s"""
std::vector<${newTypeName}> ${newListName};
for (auto &${itr2} : ${oldListName}) {
  ${newTypeName} $itr3{$update};
  ${newListName}.push_back($itr3);
}
"""
    ("//projection \n" + struct + code, RelationMetaData(Map(), s"$newListName", s"$newTypeName"))
  }
}
