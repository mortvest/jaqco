case class RelationMetaData(attributes: Map[String, String], reg: Int)

object CodeGeneration {
  def apply(physical: Physical, varName: String) = {
    def genCode(physical: Physical, relMeta: RelationMetaData): (String, RelationMetaData) = {
      physical match {
        case RangeScan(meta, from, to) => rangeScan(meta, from, to, 0)
        case IndexLookup(meta, map) => indexLookup(meta, map, relMeta.reg)
        case Filter(expr, from) =>
          val x = genCode(from, relMeta)
          (x._1 + s"Filter $expr \n", x._2)
        case OnePassProj(exprList, from) =>
          val x = genCode(from, relMeta)
          val y = onePassProj(x._2, exprList)
          (x._1 + y._1, y._2)
      }
    }
    genCode(physical, RelationMetaData(Map(), 0)) match {
      case (code, n) => code + s"auto $varName = tmp${n.reg}"
    }
  }

  def rangeScan(meta: TableMetaData, from: RangeVal, to: RangeVal, reg: Int) = {
    (s"Table: ${meta.relName} - index range scan from ${from} to${to} on index ${meta.indexParts.last}\n",
      RelationMetaData(Map(), reg))
  }

  def condTrans(expr: Expr): String = {
    expr match {
      case Attribute(attName)   => attName
      case Less(left, right)    => "("  + condTrans(left) + " < "  + condTrans(right) + ")"
      case Leq(left, right)     => "("  + condTrans(left) + " <= " + condTrans(right) + ")"
      case Greater(left, right) => "("  + condTrans(left) + " > "  + condTrans(right) + ")"
      case Geq(left, right)     => "("  + condTrans(left) + " >= " + condTrans(right) + ")"
      case Equals(left, right)  => "("  + condTrans(left) + " == " + condTrans(right) + ")"
      case Plus(left, right)    => "("  + condTrans(left) + " + "  + condTrans(right) + ")"
      case Minus(left, right)   => "("  + condTrans(left) + " - "  + condTrans(right) + ")"
      case And(left, right)     => "("  + condTrans(left) + " && " + condTrans(right) + ")"
      case Or(left, right)      => "("  + condTrans(left) + " || " + condTrans(right) + ")"
      case Not(value)           => "!(" + condTrans(value) + ")"
      case x: Const             => x.value.toString
    }
  }

  def indexLookup(meta: TableMetaData, map: Map[String, Expr], reg: Int) = {
    val relName = s"${meta.relName}"
    val listName = s"tmp${reg}"
    val keyStruct = s"${relName}_key"
    val keyType = s"${keyStruct}_type"
    val keyStructCreate =
      map.foldLeft("") ( (acc, ex) => acc + keyStruct + "." + ex._1 + " = " + condTrans(ex._2) + ";\n")
    val valStruct = s"${relName}_value"
    val valType = s"${valStruct}_type"
    val valList = s"${valStruct}_list"
    val index = s"${relName}_index"
    val code = s"""
auto $index = get_index($relName);
std::string ${relName}_value_string;
$keyType $keyStruct;
$keyStructCreate
$valType $valStruct;
std::vector<${valType}> $valList;

if (index->get(utility::encode_safe($keyStruct), ${relName}_value_string)) {
  utility::decode_safe(${relName}_value_string, ${valStruct});
  ${valList}.push_back(${valStruct});
}

"""
    (code, RelationMetaData(meta.attributes, reg))
  }

  def typeLookup(valName: String, meta: RelationMetaData) = {
    // TODO: Should be done properly when operations on multiple relations are added
    (meta.attributes get valName) match {
      case None => throw new Error(s"Type for variable $valName could not be found")
      case Some(varType) => varType
    }
  }
  def onePassProj(meta: RelationMetaData, exprList: List[Expr]) = {
    def createRelMeta(exprList: List[Expr], map: Map[String, String]): Map[String, String] = {
      exprList match {
        case Nil => map
        case x :: xs =>
          val name = getVarName(x)
          createRelMeta(xs, map) + (name -> typeLookup(name, meta))
      }
    }
    // def find(n: Int, map: Map[String, String]): Int = {
    //   val name = n.toString + varName
    //     (map get name) match {
    //     case Some(_) => find(n+1)
    //     case None => n
    //   }
    // }
    def getVarName(expr: Expr) = {
      expr match {
        case Attribute(attName) => attName
        case LongConst(value)   => s"long0"
        case StringConst(value) => s"string0"
        case _ => "" //fail here
      }
    }
    val reg = meta.reg
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
    (struct + code, RelationMetaData(Map(), outReg))
  }
}
