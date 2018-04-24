// case class TableMetaData(relName: String, indexName: String, tableNames: List[String])
// case class TableScan(meta: TableMetaData) extends Physical
// case class RangeScan(meta: TableMetaData, expr: Expr) extends Physical
// case class IndexScan(meta: TableMetaData, expr: Expr) extends Physical
// case class Filter(expr: Expr, from: Physical) extends Physical
// case class OnePassProj(exprList: List[Expr], from: Physical) extends Physical

object CodeGeneration {
  def apply(physical: Physical, varName: String) = {
    def genCode(physical: Physical, reg: Int): (String, Int) = {
      physical match {
        case TableScan(meta) => tableScan(meta, reg)
        case RangeScan(meta, expr) => rangeScan(meta, expr, reg)
        case IndexScan(meta, expr) => indexScan(meta, expr, reg)
        case Filter(expr, from) =>
          val x = genCode(from, reg)
          (x._1 + s"Filter $expr \n", x._2)
        case OnePassProj(meta, exprList, from) =>
          val x = genCode(from, reg)
          val y = onePassProj(meta, exprList, x._2)
          (x._1 + y._1, y._2)
      }
    }
    genCode(physical, 0) match {
      case (code, n) => code + s"auto $varName = tmp$n"
    }
  }
  def tableScan(meta: TableMetaData, reg: Int) = {
    (s"Table: ${meta.relName} - full scan returning tmp${reg}\n", reg)
  }

  def rangeScan(meta: TableMetaData, expr: Expr, reg: Int) = {
    (s"Table: ${meta.relName} - index range scan for: ${expr} returning tmp${reg}\n", reg)
  }

  // def createStruct(meta: TableMetaData, exprList: Option[List[Expression]], reg: Int): String = {
  //   s"struct ${meta.relName}_tmp${reg} \n"
  //   +
  //   .foldLeft("")((acc, kv) => acc + kv._1 + kv._2)
  //   +
  //   "}MACRO_PACKED;\n"
  // }

  def indexScan(meta: TableMetaData, expr: Expr, reg: Int): (String, Int) = {
    val value = expr match{
      case LongConst(x) => x.toString
      case StringConst(x) => x
      case x: Attribute => x.attName
      case _ => throw new Error("Something went horribly wrong")
    }
    val listName = s"tmp${reg}"
    val newType = s"${listName}_type"
    val valName = s"${listName}_found"
    val code = s"""
auto ${meta.relName}_index = get_index(${meta.relName});
std::string ${meta.relName}_value_string;
#define ${newType} ${meta.relName}_schema
${newType} ${valName};
std::vector<${newType}> $listName;
if (index->get(utility::encode_safe(${value}), ${meta.relName}_value_string)) {
  utility::decode_safe(${meta.relName}_value_string, ${valName});
  ${listName}.push_back(${valName});
}

"""
    (code, reg)
  }

  def typeLookup(valName: String, meta: Map[String, TableMetaData]) = {
    // TODO: Should be done properly when operations on multiple relations are added
    val lst = meta.toList
    lst match {
      case (key,TableMetaData(_, _, tables ))::x =>
        (tables get valName) match {
          case None => throw new Error(s"Type for variable $valName could not be found")
          case Some(varType) => varType
        }
      case  _ => throw new Error("Something went wrong when trying to determine the type")
    }
  }
  def onePassProj(meta: Map[String, TableMetaData], exprList: List[Expr], reg: Int) = {
    def getVarName(expr: Expr) = {
      expr match {
        case Attribute(attName) => attName
        // case LongConst(value) => s"long $value"
        // case StringConst(value) => s"char* $value"
        case _ => "" //fail here
      }
    }
    val outReg = reg + 1
    val oldListName = s"tmp${reg}"
    val newListName = s"tmp${outReg}"
    val oldTypeName = oldListName + "_type"
    val newTypeName = newListName + "_type"
    val itr = "i"
    val struct = s"struct ${newTypeName} {\n" +
    exprList.foldLeft("") ( (acc, ex) => acc + typeLookup(getVarName(ex), meta) + " " + getVarName(ex) + ";\n" ) +
    "}MACRO_PACKED;\n"
    val code = s"""
std::vector<${newTypeName}> ${newListName};
for (uint $itr = 0; $itr < ${oldListName}.size(); $itr++) {
  auto ${oldListName}_$itr = ${oldListName}[$itr];
  ${newTypeName} ${newListName}_$itr;
${exprList.foldLeft("") ((acc, ex) => acc + ("  " + newListName + "_" + itr + "." + getVarName(ex) + " = " + oldListName + "_" + itr + "." + getVarName(ex) + ";\n"))}
  ${newListName}.push_back(${newListName}_$itr);
}
"""
    (struct + code, outReg)
  }
}
