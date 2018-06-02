case class RelationMetaData(attributes: Map[String, DataType], name: String, typeName: String)
//hack to introduce call by reference
case class Ref(relNum: Int, counter: Int, fun: (Int => Unit))

import RangeScanGen._
import CodeGenFunctions._

object CodeGeneration {
  def apply(physical: Physical, varName: String, queryNum: Int, originalQuery: String) = {
    var tagNumber = 0
    def genCode(physical: Physical, relMeta: RelationMetaData): (String, RelationMetaData) = {
      val ref = Ref(queryNum, tagNumber, (tagNumber = _))
      physical match {
        case NestedLoopJoin(opList, cond) =>
          val (opCode, metaLst) =
            (opList.map {
              case x =>
                val (code, relMeta) = genCode(x, RelationMetaData(Map(), "", ""))
                (code, (getAlias(x), relMeta))
            }).unzip
          val materialization = opCode.foldLeft("") ((acc, x) => acc + x)
          val (fullCode, newMeta)  = nestedLoopJoin(cond, metaLst.toMap)
          (materialization + fullCode, newMeta)
        case RangeScan(meta, from, to, alias, projList, selectExpr) =>
          rangeScan(meta,from, to, ref, selectExpr, projList)
        case IndexLookup(meta, map, alias) => indexLookup(meta, map, ref)
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
    genCode(physical, RelationMetaData(Map(), "", "")) match {
      case (code, relMap) =>
        taggify(code + s"auto $varName = ${relMap.name};", originalQuery)
    }
  }

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
          val value = condTrans(expr, Map("" -> RelationMetaData(meta.attributes, "", "")), structName)
          // val value = condTrans(expr, Map("" -> meta), structName)
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
            case DerefExp(alias, name) => proc(alias + "_" + name, x, xs)
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

  def indexLookup(meta: TableMetaData, map: Map[String, Expr], ref: Ref) = {
    def genAssign(name: String, value: String, new_struct: String) = {
      val dest = s"${new_struct}.${name}"
        (meta.indexParts get name).get match {
          case SimpleType(_) => s"${dest} = ${value};"
          case StringType(_) => s"std::strcpy($dest, ${value});"
        }
    }
    val relName  = s"${meta.relName}"
    val listName = s"${newTag(ref)}_${relName}"
    val keyStruct = s"${listName}_key"
    val keyType = s"${relName}_key_type"
    val valueString = s"${listName}_value_string"
    val keyStructCreate = map.foldLeft("") ( (acc, ex) =>
      acc + genAssign(ex._1 ,condTrans(ex._2, Map("" -> RelationMetaData(Map(),"","")), ""), keyStruct) + "\n")
    val valStruct = s"${relName}_value"
    val valType = s"${listName}_val_type"
    val index = s"${listName}_index"
    val code = s"""
//index lookup
${newValueStruct(meta, valType)}
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

  def filter(meta: RelationMetaData, expr: Expr): (String, RelationMetaData) = {
    val oldListName = s"${meta.name}"
    val listType = s"${meta.typeName}"
    val newListName = s"${oldListName}_filter"
    val iterator = s"i"
    val code = s"""
// filter
std::vector<${listType}> ${newListName};
for (const auto &${iterator} : ${oldListName}) {
  if (${condTrans(expr, Map("" -> meta), iterator)}) {
    ${newListName}.push_back(${iterator});
  }
}
"""
    (code, RelationMetaData(meta.attributes, newListName, meta.typeName))
  }
}
