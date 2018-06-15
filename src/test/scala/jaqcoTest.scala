import jaqco._
import collection.mutable.Stack
import org.scalatest._
import better.files._
import better.files.Dsl._
import File._
import java.io.{File => JFile}
import scala.io.Source
import com.facebook.presto.sql.parser._
import com.facebook.presto.sql.tree._

object TestLogical{
  def apply(query: String) = {
    val parser = new SqlParser()
    val ast = parser.createStatement(query).asInstanceOf[Query]
    val result = LogicalPlanGenerator(ast)
    result
  }
}

object TestCondition{
  def apply(expr: String) = {
    val query = "SELECT * FROM a WHERE " + expr
    val parser = new SqlParser()
    val ast = parser.createStatement(query).asInstanceOf[Query]
    LogicalPlanGenerator(ast).asInstanceOf[Selection].cond
  }
}

object TestPhysical{
  def apply(input: RelAlg, meta: Map[String, TableMetaData]) = {
    PhysicalPlanGenerator(input, meta)
  }
}

class JaqcoTest extends FlatSpec with Matchers {
  "Logical Query Plan Generator" should "perform expression translation" in {
    assert(TestCondition("a") == Attribute("a"))
    assert(TestCondition("a < 12") == Less(Attribute("a"), Const("12", SimpleType("std::int64_t"))))
    assert(TestCondition("NOT(a < 12)") ==
      Not(Less(Attribute("a"), Const("12", SimpleType("std::int64_t")))))
  }
  it should "perform translation of SELECT statements" in {
    assert(TestLogical("SELECT * FROM a") == Rel("a", "a"))
    assert(TestLogical("SELECT * FROM b B") == Rel("b", "B"))
    assert(TestLogical("SELECT a FROM b") == Projection(List(Attribute("a")), Rel("b", "b")))
    assert(TestLogical("SELECT a FROM b,c") ==
      Projection(List(Attribute("a")), Cross(Rel("b", "b"), Rel("c", "c"))))
    assert(TestLogical("SELECT * FROM a WHERE a < 2") ==
      Selection(
        Less(
          Attribute("a"),
          Const("2", SimpleType("std::int64_t"))
        ),
        Rel("a", "a")
      )
    )
  }
  it should "fail on translation of unsupported JOIN types" in {
    assertThrows[Error] {
      TestLogical("SELECT a FROM a NATURAL JOIN b")
    }
  }
  it should "fail on translation of unsupported SELECT clauses" in {
    assertThrows[Error] {
      TestLogical("SELECT a FROM a GROUP BY a")
    }
    assertThrows[Error] {
      TestLogical("SELECT a FROM a HAVING a")
    }
    assertThrows[Error] {
      TestLogical("SELECT a FROM a ORDER BY a")
    }
  }
  /////////////////////////////////////////////
  "Physical Query Plan Generator" should
  "perform translation of logical query plan to a physical query plan" in {
    val meta = Map[String, TableMetaData](
      "test_relation" ->
        TableMetaData("test_relation", Map("user_name" -> StringType(255)),
          Map("user_name" -> StringType(255),
            "account_id" -> SimpleType("std::int64_t"),
            "balance" -> SimpleType("std::int64_t")
          )
        ),
        "other_relation" ->
        TableMetaData("other_relation", Map("account_id" -> SimpleType("std::int64_t")),
          Map("account_id" -> SimpleType("std::int64_t"),
            "value" -> SimpleType("std::int64_t")
          )
        )
    )
    val one = RangeScan(
      TableMetaData(
        "other_relation",
        Map("account_id" -> SimpleType("std::int64_t")),
        Map(
          "other_relation_account_id" -> SimpleType("std::int64_t"),
          "other_relation_value" -> SimpleType("std::int64_t")
        )
      ),
      List((SimpleType("std::int64_t"),ZeroVal())),
      List((SimpleType("std::int64_t"),MaxVal())),
      "other_relation",
      List(),
      None)

    assert(TestPhysical(Rel("other_relation", "other_relation"), meta) == one )
  }
  it should "fail on reference to an undefined columns" in {
    val meta = Map[String, TableMetaData](
      "test_relation" ->
        TableMetaData("test_relation", Map("user_name" -> StringType(255)),
          Map("user_name" -> StringType(255),
            "account_id" -> SimpleType("std::int64_t"),
            "balance" -> SimpleType("std::int64_t")
          )
        ),
      "other_relation" ->
        TableMetaData("other_relation", Map("account_id" -> SimpleType("std::int64_t")),
          Map("account_id" -> SimpleType("std::int64_t"),
            "value" -> SimpleType("std::int64_t")
          )
        )
    )
    assertThrows[Error] {
      TestPhysical(Rel("A","A"), meta)
    }
  }

}
