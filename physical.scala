final case class Error(private val message: String = "",
  private val cause: Throwable = None.orNull) extends Exception(message, cause)

case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, DataType])

sealed trait Physical
case class RangeScan(meta: TableMetaData, from: List[(DataType, RangeVal)], to: List[(DataType, RangeVal)])
    extends Physical
case class IndexLookup(meta: TableMetaData, map: Map[String, Expr]) extends Physical
case class Filter(expr: Expr, from: Physical) extends Physical
case class OnePassProj(exprList: List[Expr], from: Physical) extends Physical

object Physical{
  def apply(relAlg: RelAlg, meta: Map[String, TableMetaData]): Physical = {
    relAlg match {
      case Selection(cond, from) =>
        from match {
          case Relation(name) => translateCond(cond, getMeta(meta, name))
          case _ => Filter(cond, apply(from, meta))
        }
      case Relation(name) =>
        val tMeta = getMeta(meta, name)
        val types = tMeta.indexParts.map { case x => (tMeta.attributes get x).get }
        RangeScan(tMeta,
          (types zip tMeta.indexParts.map { case c => ZeroVal() }),
          (types zip tMeta.indexParts.map { case c => MaxVal() }))
      case Projection(attList, from) => OnePassProj(attList, apply(from, meta))
      case _ => throw new Error("Not implemented yet")
    }
  }
  def getMeta(meta: Map[String, TableMetaData], name: String) = {
    meta get name match {
      case None => throw new Error("Relation with that name does not exist")
      case Some(x) => x
    }
  }
// TODO: Fix the bug when > then < on same attribute -> one is sent to the filter. Why?
  def translateCond(cond: Expr, meta: TableMetaData) = {
    def isConstExpr (expr: Expr, meta: TableMetaData): Boolean = {
      expr match {
        case x: BinOp => isConstExpr(x.left, meta) && isConstExpr(x.right, meta)
        case x: Const => true
        case Attribute(name) => !(meta.attributes.contains(name))
        case _ => false
      }
    }
    def getRefs(expr: Expr, meta: TableMetaData): (Map[String, List[Expr]], List[Expr]) = {
      def isMatching(expr: Expr, meta: TableMetaData, attName: String): Boolean = {
        isConstExpr(expr, meta) && meta.indexParts.contains(attName)
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
        case Equals(Attribute(attName), exp) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(expr)), Nil)
        case Equals(exp, Attribute(attName)) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(Equals(Attribute(attName), exp))), Nil)
        case Less(Attribute(attName), exp) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(expr)), Nil)
        case Less(exp, Attribute(attName)) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(Less(Attribute(attName), exp))), Nil)
        case Leq(Attribute(attName), exp) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(expr)), Nil)
        case Leq(exp, Attribute(attName)) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(Leq(Attribute(attName), exp))), Nil)
        case Greater(Attribute(attName), exp) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(expr)), Nil)
        case Greater(exp, Attribute(attName)) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(Greater(Attribute(attName), exp))), Nil)
        case Geq(Attribute(attName), exp) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(expr)), Nil)
        case Geq(exp, Attribute(attName)) if isMatching(exp, meta, attName) =>
          (Map(attName -> List(Geq(Attribute(attName), exp))), Nil)
        case x => (Map[String, List[Expr]](), List(x))
      }
    }
    def findG(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Greater(Attribute(name), expr)  => (Some(expr), lst)
          case Geq(Attribute(name), expr)      => (Some(expr), xs)
          case _ => val y = findG(xs); (y._1, x::y._2)
        }
        case Nil => (None, Nil)
      }
    }

    def findL(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Less(Attribute(name), expr) => (Some(expr), lst)
          case Leq(Attribute(name), expr)  => (Some(expr), xs)
          // case _ => val y = findL(xs); (y._1, lst)
          case _ => val y = findL(xs); (y._1, x::y._2)
        }
        case Nil => (None, Nil)
      }
    }
    def findE(lst: List[Expr]): Option[Expr] = {
      lst match {
        case x::xs => x match {
          case Equals(Attribute(name), expr) => Some(expr)
          case Equals(expr, Attribute(name)) => Some(expr)
          case _ => findE(xs)
        }
        case Nil => None
      }
    }
    def findN(lst: List[Expr]): (Option[Expr], List[Expr]) = {
      lst match {
        case x::xs => x match {
          case Equals(Attribute(name), expr) => (Some(expr), xs)
          case Equals(expr, Attribute(name)) => (Some(expr), xs)
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
              case None => println(s"FOUND: ${findG(exList)}"); findG(exList) match {
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
          case _ => BooleanConst(true)
        }
      }
      val result = foo(lst)
      if (result == BooleanConst(true)) None else Some(result)
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
    val (mapping, restLst) = getRefs(cond, meta)
    lookup(meta.indexParts, mapping) match {
      case Some(k) =>
        andify(mapToList(k._2) ::: restLst) match {
          case None => IndexLookup(meta, (meta.indexParts zip k._1).toMap)
          case Some(expr) => Filter(expr, IndexLookup(meta, (meta.indexParts zip k._1).toMap))
        }
      case None =>
        val types = meta.indexParts.map { case x => (meta.attributes get x).get }
        val (fromLst, fromMap) = getFrom(meta.indexParts, mapping)
        val (toLst, toMap) = getTo(meta.indexParts, fromMap)
        andify(mapToList(toMap) ::: restLst) match {
          case None => RangeScan(meta, (types zip fromLst), (types zip toLst))
          case Some(expr) => Filter(expr, RangeScan(meta, (types zip fromLst), (types zip toLst)))
        }
    }
  }
}
