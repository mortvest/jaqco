// case class TableMetaData(relName: String, indexName: String, tableNames: List[String])

// case class TableScan(meta: TableMetaData) extends Physical
// case class RangeScan(meta: TableMetaData, expr: Expr) extends Physical
// case class IndexScan(meta: TableMetaData, expr: Expr) extends Physical
// case class Filter(expr: Expr, from: Physical) extends Physical
// case class OnePassProj(exprList: List[Expr], from: Physical) extends Physical

object CodeGeneration {
  def apply(physical: Physical): String = {
    physical match {
      case TableScan(meta) => tableScan(meta)
      case RangeScan(meta, expr) => rangeScan(meta, expr)
      case IndexScan(meta, expr) => indexScan(meta, expr)
      case Filter(expr, from) => apply(from) + s"Filter $expr \n"
      case OnePassProj(exprList, from) => apply(from) + s"One Pass Projection for $exprList \n"
    }
  }
  def tableScan(meta: TableMetaData) = {
    s"Table: ${meta.relName} - full scan \n"
  }

  def rangeScan(meta: TableMetaData, expr: Expr) = {
    s"Table: ${meta.relName} - index range scan for: ${expr}\n"
  }

  // add token name system: tmp1, tmp2
  def indexScan(meta: TableMetaData, expr: Expr) = {
    // load index?
    val index = "\nauto index = get_index(ACCOUNT_RELATION);\n"
    // Get metadata here?
    val schema = s"""
      struct ${meta.relName}_schema {
        customer_id_t customer_id;
      }MACRO_PACKED;
"""
    val selection = s"""
    std::string value_string;
    schema customer_schema;
    index->get(utility::encode_safe(customer_name), value_string)
    utility::decode_safe(value_string, customer_schema);
"""
    s"Table ${meta.relName} - index scan on ${meta.indexName} and ${expr} \n"
  }

  def onePassProj(attList: List[Any]) = {
    def singeAttrib(att: Any) = {
"""
    auto customer_id = customer_schema.customer_id;
    auto customer_id_key = utility::encode_safe(customer_id);
"""
    }
  }
}
