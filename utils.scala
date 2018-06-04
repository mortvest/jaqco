object Utils {
  def isConstExpr (expr: Expr, meta: TableMetaData): Boolean = {
    expr match {
      case x: BinOp => isConstExpr(x.left, meta) && isConstExpr(x.right, meta)
      case _ @ (_: Const | _: OutsideVar) => true
      case _ => false
    }
  }
  def checkDuplicates(lst: List[String], what: String) = {
    lst.groupBy(identity).filter{ case x => (x._1 != "") && (x._2.size > 1) }.headOption match {
      case Some((name, _)) => throw new Error(s"""$what \"$name\" is given twice""")
      case None => {}
    }
  }
}
