package com.rokim.db.adapters.fs

import com.rokim.db.Domain.{AutoInc, BOOLEAN, Column, ColumnValue, Greater, INT, IntColumnFilter, LONG, LineValue, VARCHAR, given_GreaterOperator_Int}
import com.rokim.db.Utils

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class DbAdaptorImplTest extends munit.FunSuite {
  // Test table schema
  val tableName = "testDb1"
  val idCol = Column("id", LONG)
  val ageCol = Column("AGE", INT)
  val nameCol = Column("NAME", VARCHAR(8))
  val activeCol = Column("ACTIVE", BOOLEAN)
  val cols = Seq(idCol, ageCol, nameCol, activeCol)
  val pk = Seq(idCol)

  // Fixture for database directory and adapter
  case class DbFixtureContext(adapter: DbAdapterImpl, dbPath: Path)

  val dbFixture = FunFixture[DbFixtureContext](
    setup = { _ =>
      val dbDir = Files.createTempDirectory("testDb")
      DbFixtureContext(new DbAdapterImpl(dbDir), dbDir)
    },
    teardown = { _ => () }
  )

  // Helper methods
  private def createTestTable(dbAdapter: DbAdapterImpl): Unit = {
    dbAdapter.createTable(tableName, cols, pk, Some(idCol))
  }

  private def insertTestData(dbAdapter: DbAdapterImpl): Unit = {
    dbAdapter.insertData(tableName,
      Seq(idCol.valueOf(1), ageCol.valueOf(10), nameCol.valueOf("abc"), activeCol.valueOf(true)))
    dbAdapter.insertData(tableName,
      Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCol.valueOf("romain"), activeCol.valueOf(false)))
  }

  private def getExpectedTestData: Seq[LineValue] = {
    Seq(
      LineValue(Seq(idCol.valueOf(1), ageCol.valueOf(10), nameCol.valueOf("abc"), activeCol.valueOf(true))),
      LineValue(Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCol.valueOf("romain"), activeCol.valueOf(false)))
    )
  }

  private def getExpectedBytes: List[Byte] = {
    (Utils.longToBytes(1) ++ Utils.intToBytes(10) ++ Utils.paddedStringToBytes("abc", 8) ++ Array(Utils.boolToByte(true)) ++
      Utils.longToBytes(300) ++ Utils.intToBytes(11) ++ Utils.paddedStringToBytes("romain", 8) ++ Array(Utils.boolToByte(false))).toList
  }

  test("Write metadata") {
    val dbsDirectory = Files.createTempDirectory("testDbs")
    val dbAdapter = new DbAdapterImpl(dbsDirectory)

    dbAdapter.createTable(tableName, cols, pk, Some(idCol))

    val writtenFile = new String(Files.readAllBytes(dbsDirectory.resolve(tableName).resolve("metadata")), StandardCharsets.UTF_8)
    val expectedMetadata =
      s"""{
         |  "name" : "$tableName",
         |  "columns" : [
         |    {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    },
         |    {
         |      "name" : "AGE",
         |      "colType" : {
         |        "type" : "INT"
         |      }
         |    },
         |    {
         |      "name" : "NAME",
         |      "colType" : {
         |        "type" : "VARCHAR",
         |        "size" : 8
         |      }
         |    },
         |    {
         |      "name" : "ACTIVE",
         |      "colType" : {
         |        "type" : "BOOLEAN"
         |      }
         |    }
         |  ],
         |  "pk" : [
         |    {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    }
         |  ],
         |  "autoInc" : {
         |    "column" : {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    },
         |    "count" : 0
         |  },
         |  "indexes" : [
         |  ]
         |}""".stripMargin

    assertEquals(writtenFile, expectedMetadata)

    val dataFile = dbsDirectory.resolve(tableName).resolve("data")
    assert(dataFile.toFile.isFile)
  }

  test("Read metadata") {
    val metadataContent =
      s"""{
         |  "name" : "$tableName",
         |  "columns" : [
         |    {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    },
         |    {
         |      "name" : "AGE",
         |      "colType" : {
         |        "type" : "INT"
         |      }
         |    },
         |    {
         |      "name" : "NAME",
         |      "colType" : {
         |        "type" : "VARCHAR",
         |        "size" : 8
         |      }
         |    },
         |    {
         |      "name" : "ACTIVE",
         |      "colType" : {
         |        "type" : "BOOLEAN"
         |      }
         |    }
         |  ],
         |  "pk" : [
         |    {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    }
         |  ],
         |  "autoInc" : {
         |    "column" : {
         |      "name" : "id",
         |      "colType" : {
         |        "type" : "LONG"
         |      }
         |    },
         |    "count" : 67
         |  },
         |  "indexes" : [
         |  ]
         |}""".stripMargin

    val dbDirectory = Files.createTempDirectory("testDb")
    Files.write(dbDirectory.resolve("metadata"), metadataContent.getBytes(StandardCharsets.UTF_8))

    val metadata = TableMetadata.read(dbDirectory)

    val expectedMetadata = TableMetadata(tableName, cols, pk, Some(AutoInc(idCol, 67)), Nil)
    assertEquals(metadata, expectedMetadata)
  }

  dbFixture.test("Insert data") { ctx =>
    createTestTable(ctx.adapter)
    insertTestData(ctx.adapter)

    val data = Files.readAllBytes(ctx.dbPath.resolve(tableName).resolve("data")).toList
    assertEquals(data, getExpectedBytes)
  }

  dbFixture.test("Read data") { ctx =>
    createTestTable(ctx.adapter)
    insertTestData(ctx.adapter)

    val data = ctx.adapter.getAll(tableName)
    assertEquals(data, getExpectedTestData)
  }

  dbFixture.test("Read filtered data") { ctx =>
    createTestTable(ctx.adapter)
    insertTestData(ctx.adapter)

    // Filter: AGE > 10 (should only return the second row with age=11)
    val ageFilter = IntColumnFilter("AGE", Greater[Int](), 10)
    val data = ctx.adapter.getFiltered(tableName, ageFilter)

    val expected = Seq(
      LineValue(Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCol.valueOf("romain"), activeCol.valueOf(false)))
    )
    assertEquals(data, expected)
  }
}