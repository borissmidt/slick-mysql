package com.foerstertechnologies.slickmysql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import slick.lifted.{ProvenShape, TableQuery, Tag}
import ExMySQLProfile.api._
class ExMySQLProfileSpec extends AnyFlatSpec with Matchers {
  "the native upsert builder" should "create a valid my-sql query" in {
    val sql = ExMySQLProfile.compileInsert(TableQuery[TestTable].toNode).upsert.sql
    sql.contains("(`id`,`start`,`end`,`status`)") shouldBe true
    sql.contains("(?,?,?,?)") shouldBe true
    sql.contains("`id` = values(`id`)") shouldBe true
    sql.contains("`start` = values(`start`)") shouldBe true
    sql.contains("`end` = values(`end`)") shouldBe true
    sql.contains("`status` = values(`status`)") shouldBe true
  }
}

case class Row(
    id: Int,
    start: Long,
    end: Long,
    status: String
  )

class TestTable(tag: Tag) extends Table[Row](tag, "TestTable") {
  val id = column[Int]("id")
  val start = column[Long]("start")
  val end = column[Long]("end")
  val status = column[String]("status")

  override def * : ProvenShape[Row] =
    (
      id,
      start,
      end,
      status
    ) <> (
      (Row.apply _).tupled,
      Row.unapply
    )
}
