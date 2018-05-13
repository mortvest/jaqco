object PlanGenerator{
  def translateCond(cond: Expr, meta: TableMetaData, tableAlias: String): (Physical, Option[Expr]) = {
    def isConstExpr (expr: Expr, meta: TableMetaData): Boolean = {
      expr match {
        case x: BinOp => isConstExpr(x.left, meta) && isConstExpr(x.right, meta)
        case x: Const => true
        case _ => false
      }
    }
    def getRefs(expr: Expr, meta: TableMetaData): (Map[String, List[Expr]], List[Expr]) = {
      def isMatching(expr: Expr, meta: TableMetaData, attName: String, aliasName: String): Boolean = {
        isConstExpr(expr, meta) && meta.indexParts.contains(attName) && aliasName == tableAlias
      }
      def merge(fst: Map[String, List[Expr]], snd: Map[String, List[Expr]]) = {
        val merged = fst.toList ::: snd.toList
        val grouped =  merged.groupBy(_._1)
        grouped.map{ case (name, x) => (name -> (x.foldLeft(List[Expr]()) ( (acc, y) => y._2 ::: acc )))}
      }
      expr match {
        case And(left, right) =>
          val lf = getRefs(left, meta)
          val rt = getRefs(right, meta)
          ((merge(lf._1, rt._1)), (lf._2 ::: rt._2))
        case Equals(DerefExp(alias, attName), exp) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(expr)), Nil)
        case Equals(exp, DerefExp(alias, attName)) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(Equals(DerefExp(alias, attName), exp))), Nil)
        case Less(DerefExp(alias, attName), exp) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(expr)), Nil)
        case Less(exp, DerefExp(alias, attName)) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(Less(DerefExp(alias, attName), exp))), Nil)
        case Leq(DerefExp(alias, attName), exp) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(expr)), Nil)
        case Leq(exp, DerefExp(alias, attName)) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(Leq(DerefExp(alias, attName), exp))), Nil)
        case Greater(DerefExp(alias, attName), exp) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(expr)), Nil)
        case Greater(exp, DerefExp(alias, attName)) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(Greater(DerefExp(alias, attName), exp))), Nil)
        case Geq(DerefExp(alias, attName), exp) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(expr)), Nil)
        case Geq(exp, DerefExp(alias, attName)) if isMatching(exp, meta, attName, alias) =>
          (Map(attName -> List(Geq(DerefExp(alias, attName), exp))), Nil)
        case x => (Map[String, List[Expr]](), List(x))
      }
    }
    def findG(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Greater(DerefExp(_, name), expr)  => (Some(expr), lst)
          case Geq(DerefExp(_, name), expr)      => (Some(expr), xs)
          case _ => val y = findG(xs); (y._1, x::y._2)
        }
        case Nil => (None, Nil)
      }
    }

    def findL(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Less(DerefExp(_, name), expr) => (Some(expr), lst)
          case Leq(DerefExp(_, name), expr)  => (Some(expr), xs)
          // case _ => val y = findL(xs); (y._1, lst)
          case _ => val y = findL(xs); (y._1, x::y._2)
        }
        case Nil => (None, Nil)
      }
    }
    def findE(lst: List[Expr]): Option[Expr] = {
      lst match {
        case x::xs => x match {
          case Equals(DerefExp(_, name), expr) => Some(expr)
          case Equals(expr, DerefExp(_, name)) => Some(expr)
          case _ => findE(xs)
        }
        case Nil => None
      }
    }
    def findN(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Equals(DerefExp(_, name), expr) => (Some(expr), xs)
          case Equals(expr, DerefExp(_, name)) => (Some(expr), xs)
          // case _ => val y = findL(xs); (y._1, lst)
          case _ => val y = findN(xs); (y._1, x::y._2)
        }
        case Nil => (None, Nil)
      }
    }
    def getFrom(indexParts: List[String], map: Map[String, List[Expr]]) = {
      def iter(lst: List[String], map: Map[String, List[Expr]]):
          (List[RangeVal], Map[String, List[Expr]]) = {
        lst match {
          case x::xs => (map get x) match {
            case None => (ZeroVal() :: xs.map{ case x => ZeroVal() }, map)
            case Some(exList) => findE(exList) match {
              case Some(ex) =>
                val (newList, newMap) = iter(xs, map)
                (ConstVal(ex) :: newList, newMap)
              case None => findG(exList) match {
                case (None, _) => (ZeroVal() :: xs.map{ case x => ZeroVal() }, map)
                case (Some(y), newList) => (ConstVal(y) :: xs.map{ case x => ZeroVal() },
                  ((map - x) + (x -> newList)))
              }
            }
          }
          case Nil => (Nil, map)
        }
      }
      iter(indexParts, map)
    }

    def getTo(indexParts: List[String], map: Map[String, List[Expr]]) = {
      def iter(lst: List[String], map: Map[String, List[Expr]]):
          (List[RangeVal], Map[String, List[Expr]]) = {
        lst match {
          case x::xs => (map get x) match {
            case None => (MaxVal() :: xs.map{ case x => MaxVal() }, map)
            case Some(exList) =>
              val (foo, fLst) = findN(exList)
              foo match {
                case Some(ex) =>
                  val (newList, newMap) = iter(xs, (map - x) + (x -> fLst))
                  (ConstVal(ex) :: newList, newMap)
                case None => findL(exList) match {
                  case (None, _) => (MaxVal() :: xs.map{ case x => MaxVal() }, (map - x) + (x -> fLst))
                  case (Some(y), newList) => (ConstVal(y) :: xs.map{ case x => MaxVal() },
                    ((map - x) + (x -> newList)))
                }
              }
          }
          case Nil => (Nil, map)
        }
      }
      iter(indexParts, map)
    }
    def mapToList(map: Map[String, List[Expr]]) = {
      map.foldLeft (List[Expr]()) { case (acc, y) => y._2 ::: acc }
    }
    def andify (lst: List[Expr]): Option[Expr] = {
      def foo (lst: List[Expr]): Expr = {
        lst match {
          case x::Nil => x
          case x::xs => And(x, foo(xs))
          case _ => Const("true", SimpleType("bool"))
        }
      }
      val result = foo(lst)
      if (result == Const("true", SimpleType("bool"))) None else Some(result)
    }
    def lookup(indexParts: List[String], map: Map[String, List[Expr]]):
        Option[(List[Expr], Map[String, List[Expr]])] = {
      def fun(indexParts: List[String], map: Map[String, List[Expr]]):
          (List[Expr], Map[String, List[Expr]]) = {
        indexParts match {
          case x::xs => (map get x) match {
            case None => fun(xs, map)
            case Some(y) =>
              val (expr, list) = findN(y)
              expr match {
                case None => fun(xs, map)
                case Some(ex) =>
                  val newMap = (map - x) + (x -> list)
                  val j = fun(xs, newMap)
                  (ex :: j._1, j._2)
              }
          }
          case Nil => (Nil, map)
        }
      }
      val maps = fun(indexParts, map)
      if (maps._1.size == indexParts.size) Some(maps) else None
    }
    val aMeta = {
      val attributes = meta.attributes.map { case x => (tableAlias + "_" + x._1 -> x._2) }
      val indexParts = meta.indexParts.map { case x => (tableAlias + "_" + x) }
      TableMetaData(meta.relName, indexParts, attributes)
    }
    val (mapping, restLst) = getRefs(cond, meta)
    lookup(meta.indexParts, mapping) match {
      case Some(k) =>
        andify(mapToList(k._2) ::: restLst) match {
          case None => (IndexLookup(aMeta, (aMeta.indexParts zip k._1).toMap, tableAlias), None)
          case Some(expr) =>
            (IndexLookup(aMeta, (aMeta.indexParts zip k._1).toMap, tableAlias), Some(expr))
        }
      case None =>
        val types = meta.indexParts.map { case x => (meta.attributes get x).get }
        val (fromLst, fromMap) = getFrom(meta.indexParts, mapping)
        val (toLst, toMap) = getTo(meta.indexParts, fromMap)
        andify(mapToList(toMap) ::: restLst) match {
          case None => (RangeScan(aMeta, (types zip fromLst), (types zip toLst), tableAlias), None)
          case Some(expr) => (RangeScan(aMeta, (types zip fromLst), (types zip toLst), tableAlias), Some(expr))
        }
    }
  }
}
