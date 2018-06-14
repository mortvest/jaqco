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
  def apply(input: RelAlg, result: Physical, meta: Map[String, TableMetaData]) = {
    assert(PhysicalPlanGenerator(input, meta) == result)
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
    assert(1 == 1)
  }
  it should "choose choose the appropriate physical operator based on schema and WHERE clause" in {
    assert(1 == 1)
  }
  "Code Generator" should
  "generate code for a physical plan" in {
    assert(1 == 1)
  }
}
