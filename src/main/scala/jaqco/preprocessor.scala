package jaqco
import Utils._

object Preprocessor {
  def removeComments(input: String) = {
    val queryPattern = """/\*(.|\n)*?\*/|//.*""".r
    queryPattern.replaceAllIn(input, "")
  }
  def processDDL(input: String) = {
    // val query = raw"<q\*>(.*?)<\*q>".r
    val query = """(?s)(CREATE TABLE.*?);""".r
    query.findAllIn(removeComments(input)).map{case query(x) => x}.toList
  }

  def processFile(inputStr: String, fileRef: SimpleRef, meta: Map[String, TableMetaData]): String = {
    val input = removeComments(inputStr)
    var counter = fileRef.counter
    def getQueryNum(ref: SimpleRef): Int = {
      val oldValue = ref.counter
      val newValue = oldValue + 1
      ref.fun(newValue)
      oldValue
    }
    def choose(name: String, query: String) = {
      val ref = SimpleRef(counter, (counter = _))
      name match {
        case retVal => StatementProcessor(query, getQueryNum(ref), retVal, meta)
      }
    }
    val queryPattern = """(?s)<q\*([A-Za-z0-9_]*)>(.*?)<\*q>""".r
    val variables = (for (m <- queryPattern.findAllMatchIn(input)) yield m.group(1)).toList
    checkDuplicates(variables, "Output variable")
    val code = queryPattern.replaceAllIn(input, _ match {
      case queryPattern(x,y) => choose(x, y)
      })
    fileRef.fun(counter)
    code
    // def apply(query: String, queryNum: Int, retName: String, meta: Map[String, TableMetaData]) = {
  }
}
