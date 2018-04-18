// sealed trait RelAlg
// case class Relation(relName: String) extends RelAlg
// case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
// case class Selection(cond: Expr, from: RelAlg) extends RelAlg
// case class Cross(left: RelAlg, right: RelAlg) extends RelAlg
// case class NaturalJoin(left: RelAlg, right: RelAlg) extends RelAlg

sealed trait Physical
// full table scan, need a index name
case class TableScan(relName: String, indexName: String) extends Physical
case class RangeScan(relName: String, expr: Expr) extends Physical
case class IndexScan(relName: String, expr: Expr, indexName: String) extends Physical
case class Filter(expr: Expr, from: Physical) extends Physical
case class OnePassProj(exprList: List[Expr], from: Physical) extends Physical

object Physical{
  def apply(relAlg: RelAlg): Physical = {
    relAlg match {
      case Selection(cond, from) =>
        from match {
          case Relation(name) => translateCond(cond, name)
          case _ => throw new Error("Not implemented yet")
        }
      case Relation(name) => TableScan(name, findIndex(name))
      case Projection(attList, from) => OnePassProj(attList, apply(from))
      case _ => throw new Error("Not implemented yet")
    }
  }
  // TODO: Implement. Maybe get from metadata?
  def findIndex(relname: String) = {"a"}
  def translateCond(cond: Expr, relName: String): Physical = {
    cond match {
      // Only if attribute is indexed
      case Equals(Attribute(attName),(x: Const)) if (findIndex(relName) == attName) =>
        IndexScan(relName, cond, relName)
      case Equals((x: Const), Attribute(attName)) if (findIndex(relName) == attName) =>
        IndexScan(relName, cond, attName)
      // Otherwize do full table scan and then filter with the expression
      case _ => Filter(cond, TableScan(relName, findIndex(relName)))
    }
  }
}
