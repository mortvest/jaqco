final case class Error(private val message: String = "",
  private val cause: Throwable = None.orNull) extends Exception(message, cause)

case class TableMetaData(relName: String, indexParts: List[String], attributes: Map[String, String])

sealed trait Physical
case class RangeScan(meta: TableMetaData, from: RangeVal, to: RangeVal) extends Physical
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
      case Relation(name) => RangeScan(getMeta(meta, name), ZeroVal(), MaxVal())
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
  // checks if the expression terms are constants
  def translateCond(cond: Expr, meta: TableMetaData): Physical = {
    def isConstExpr (expr: Expr, meta: TableMetaData): Boolean = {
      expr match {
        case x: BinOp => isConstExpr(x.left, meta) && isConstExpr(x.right, meta)
        case x: Const => true
        case Attribute(name) => !(meta.attributes.contains(name))
        case _ => false
      }
    }
    def getRefs(expr: Expr, meta: TableMetaData):
        (List[(String, Expr)], List[(String, Expr)], List[Expr]) = {
      expr match {
        case And(left, right) =>
          val lf = getRefs(left, meta)
          val rt = getRefs(right, meta)
          ((lf._1 ::: rt._1), (lf._2 ::: rt._2), (lf._3 ::: rt._3))
          // TODO: WHERE a > 2, b = 2 (if b is key) Should be a index scan, followed by filter
          // TODO:                    (if a is key) Should be a ranged scan, followed by filter
        case Equals(Attribute(attName), expr) if isConstExpr(expr, meta) =>
          (List((attName, expr)), Nil, Nil)
        case Equals(expr, Attribute(attName)) if isConstExpr(expr, meta) =>
          (List((attName, expr)), Nil, Nil)
          // TODO: WHERE a > 2 and b = 2 IF a,b is a composite key - still a range scan
          // ???: Also important which part of the key is first
        case x: RangeOp => (x.left, x.right) match {
          case (Attribute(attName), expr) if isConstExpr(expr, meta) =>
            (Nil, List((attName, x)), Nil)
          case (expr, Attribute(attName)) if isConstExpr(expr, meta) =>
            (Nil, List((attName, x)), Nil)
        }
        case x => (List(), List(), List(x))
      }
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
      if (result == BooleanConst(true)) {
        None
      } else {
        Some(result)
      }
    }

    def fst[A, B](lst: List[(A, B)]) = {lst.map(x => x._1)}
    def snd[A, B](lst: List[(A, B)]) = {lst.map(x => x._2)}

    def lookup(lookup: List[(String, Expr)], range: List[(String, Expr)], rest: List[Expr]) = {
      def getNewLst(lst1: List[(String, Expr)], lst2: List[String]):
          (List[(String, Expr)], List[(String, Expr)]) = {
        (lst1, lst2) match {
          case (Nil, _) => (Nil, Nil)
          case (xs, Nil) => (Nil, xs)
          case (x::xs, ys) if ys.contains(x._1) =>
            val rec = getNewLst(xs, ys.filter(z => z != x._1))
            (x::rec._1, rec._2)
          case (x::xs, ys) => getNewLst(xs, ys)
        }
      }
      val index = meta.indexParts
      if (index.forall(x => fst(lookup).contains(x))) {
        val (newLookup, newRest) = getNewLst(lookup, index)
        andify(rest ::: snd(range) ::: snd(newRest)) match {
          case Some(x) => Some(Filter(x, IndexLookup(meta, newLookup.toMap)))
          case None => Some(IndexLookup(meta, newLookup.toMap))
        }
      } else {
        None
      }
    }
    // TODO: Difference between <= and <
    def range(lookup: List[(String, Expr)], range: List[(String, Expr)], rest: List[Expr]) = {
      def getFrom(index: String, lst: List[(String, Expr)]): (Option[Expr], List[(String, Expr)]) = {
        lst match {
          case (name, ex)::xs if (name == index) => ex match {
            case Greater(Attribute(name), expr)  => (Some(expr), xs)
            case Greater(expr, Attribute(name))  => (Some(expr), xs)
            case Geq(Attribute(name), expr)      => (Some(expr), xs)
            case Geq(expr, Attribute(name))      => (Some(expr), xs)
            case _ =>
              val (found, list) = getFrom(index,xs)
              (found, (name, ex)::list)
          }
          case Nil => (None, lst)
          case (name, ex)::xs =>
            val (found, list) = getFrom(index,xs)
            (found, (name, ex)::list)
        }
      }
      def getTo(index: String, lst: List[(String, Expr)]): (Option[Expr], List[(String, Expr)]) = {
        lst match {
          case (name, ex)::xs if (name == index) => ex match {
            case Less(Attribute(name), expr)     => (Some(expr), xs)
            case Less(expr, Attribute(name))     => (Some(expr), xs)
            case Leq(Attribute(name), expr)      => (Some(expr), xs)
            case Leq(expr, Attribute(name))      => (Some(expr), xs)
            case _ =>
              val (found, list) = getTo(index,xs)
              (found, (name, ex)::list)
          }
          case Nil => (None, lst)
          case (name, ex)::xs =>
            val (found, list) = getTo(index,xs)
            (found, (name, ex)::list)
        }
      }
      def addFilter(value: Physical, cond: Option[Expr]): Physical = {
        cond match {
          case None => value
          case Some(c) => Filter(c, value)
        }
      }
      if (meta.indexParts.size != 1) {
        None
      } else {
        val ind = meta.indexParts.last
        val (from, fromList) = getFrom(ind, range)
        val (to, toList) = getTo(ind, fromList)
        val filter = andify(rest ::: snd(lookup) ::: snd(toList))
          (from, to) match {
          case (None, None)           => None
          case (Some(ex), None)       => Some(addFilter(RangeScan(meta, ConstVal(ex), MaxVal()), filter))
          case (None, Some(ex))       => Some(addFilter(RangeScan(meta, ZeroVal(), ConstVal(ex)), filter))
          case (Some(ex1), Some(ex2)) =>
            Some(addFilter(RangeScan(meta, ConstVal(ex1), ConstVal(ex2)),filter))
        }
      }
    }
    val (ll, rl, rt) = getRefs(cond, meta)
    val lookupPlan = lookup(ll, rl, rt)
    val rangePlan  = range(ll, rl, rt)
    if (lookupPlan != None) {
      lookupPlan.get
    }else if(rangePlan != None) {
      rangePlan.get
    }else{
      Filter(cond, RangeScan(meta, ZeroVal(), MaxVal()))
    }
  }
}
