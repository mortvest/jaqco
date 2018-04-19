// sealed trait RelAlg
// case class Relation(relName: String) extends RelAlg
// case class Projection(attList: List[Expr], from: RelAlg) extends RelAlg
// case class Selection(cond: Expr, from: RelAlg) extends RelAlg
// case class Cross(left: RelAlg, right: RelAlg) extends RelAlg
// case class NaturalJoin(left: RelAlg, right: RelAlg) extends RelAlg

final case class Error(private val message: String = "",
  private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

case class TableMetaData(relName: String, indexName: String, tableNames: List[String])

sealed trait Physical
case class TableScan(meta: TableMetaData) extends Physical
case class RangeScan(meta: TableMetaData, expr: Expr) extends Physical
case class IndexScan(meta: TableMetaData, expr: Expr) extends Physical
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
      case Relation(name) => TableScan(getMeta(meta, name))
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
  def translateCond(cond: Expr, meta: TableMetaData): Physical = {
    cond match {
      // TODO: add support for range scans
      case Equals(Attribute(attName1), Attribute(attName2))
          if (meta.indexName == attName1) && !(meta.tableNames.contains(attName2)) =>
        IndexScan(meta, cond)
      case Equals(Attribute(attName1), Attribute(attName2))
          if (meta.indexName == attName2) && !(meta.tableNames.contains(attName1)) =>
        IndexScan(meta, cond)
      // Only if attribute is indexed
      case Equals(Attribute(attName),(x: Const)) if (meta.indexName == attName) =>
        IndexScan(meta, cond)
      case Equals((x: Const), Attribute(attName)) if (meta.indexName == attName) =>
        IndexScan(meta, cond)
      // Otherwize do full table scan and then filter using the expression
      case _ => Filter(cond, TableScan(meta))
    }
  }
}
