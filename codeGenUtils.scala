object CodeGenUtils {
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
        (l, r) match {
        case (StringType(_), StringType(_)) => SimpleType("bool")
        case (leftType, rightType) =>
          if (l != r) throw new Error("Comparison is only allowed between values of the same type")
          else SimpleType("bool")
      }
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
              case Some(meta) =>
                getType(meta)
            }
          case Some(meta) => getType(meta)
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
        case Mult(left, right)    => "("  + trans(left) + " * "  + trans(right) + ")"
        case Div(left, right)     => "("  + trans(left) + " / "  + trans(right) + ")"
        case Mod(left, right)     => "("  + trans(left) + " % "  + trans(right) + ")"
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
  def structElem (tup: (String, DataType), init: String) = {
    tup._2 match {
      case SimpleType(typ) => s"$typ ${tup._1}${init};"
      case StringType(len) => s"char ${tup._1}[$len]${init};"
    }
  }
  def newValueStruct(meta: TableMetaData, valType: String) = {
    s"struct ${valType} {\n" + meta.attributes.foldLeft("") ((acc, x) =>
      acc + "  " + structElem(x,"") + "\n")+ "}MACRO_PACKED;\n"
  }
  def taggify(code: String, query: String) = {
    val tag = "////////////////////////////////// GENERATED BY JAQCO ///////////////////////////////////"
    query match {
      case "" => s"${tag}\n${code}\n${tag}"
      case query =>
        val ogQuery = s"""//${query}"""
        s"${tag}\n${ogQuery}\n${code}\n${tag}"
    }
  }
}
