package com.rokim.db.adapters.fs

import com.rokim.db.Domain.{AutoInc, BOOLEAN, Column, ColumnValue, INT, LONG, LineValue, VARCHAR}
import com.rokim.db.adapters.fs.DbAdapterImpl
import DbAdapterImpl.TableMetadata
import com.rokim.db.Utils

import java.nio.file.Files

class DbCreatorImplTest extends munit.FunSuite {
  val tableName = "testDb1"
  val idCol = Column("id", LONG)
  val ageCol = Column("AGE", INT)
  val nameCOl = Column("NAME", VARCHAR(8))
  val activCol = Column("ACTIVE", BOOLEAN)
  val cols = Seq(idCol, ageCol, nameCOl, activCol)
  val pk = Seq(idCol)

  def getDbDirectory = Files.createTempDirectory("testDb")


  test("Write metadata") {
    val dbsDirectory = Files.createTempDirectory("testDbs")
    println()
    val dbCreator = new DbAdapterImpl(dbsDirectory)
    dbCreator.createTable(tableName, cols, pk, Some(idCol))
    val writtenFile = Files.readString(dbsDirectory.resolve("testDb1").resolve("metadata"))
    assertEquals(writtenFile,
      s"""testDb1
         |AUTOINC id 0
         |id
         |id LONG
         |AGE INT
         |NAME VARCHAR 8
         |ACTIVE BOOLEAN
         |""".stripMargin)
    val dataFile = dbsDirectory.resolve("testDb1").resolve("data")
    assert(dataFile.toFile.isFile)
  }

  test("Read metadata") {
    val expectedFile =
      s"""testDb1
         |AUTOINC id 67
         |id
         |id LONG
         |AGE INT
         |NAME VARCHAR 8
         |ACTIVE BOOLEAN
         |""".stripMargin
    val dbDirectory = Files.createTempDirectory("testDb")
    Files.writeString(dbDirectory.resolve("metadata"), expectedFile)
    val dbCreator = new DbAdapterImpl(dbDirectory)
    val metadata = dbCreator.readTableMetadata(dbDirectory)
    assertEquals(metadata, TableMetadata("testDb1", cols, pk, Some(AutoInc(idCol, 67))))
  }

  test("Insert data") {
    val dbDir = getDbDirectory
    val dbCreator = new DbAdapterImpl(dbDir)
    dbCreator.createTable(tableName, cols, pk, Some(idCol))
    dbCreator.insertData(tableName, Seq(idCol.valueOf(1), ageCol.valueOf(10), nameCOl.valueOf("abc"), activCol.valueOf(true)))
    dbCreator.insertData(tableName, Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCOl.valueOf("romain"), activCol.valueOf(false)))
    val data = Files.readAllBytes(dbDir.resolve(tableName).resolve("data")).toList
    val expected = (Utils.longToBytes(1) ++ Utils.intToBytes(10) ++ Utils.paddedStringToBytes("abc", 8) ++ Array(Utils.boolToByte(true)) ++
      Utils.longToBytes(300) ++ Utils.intToBytes(11) ++ Utils.paddedStringToBytes("romain", 8) ++ Array(Utils.boolToByte(false))).toList
    assertEquals(data, expected)
  }

  test("Read data") {
    val dbDir = getDbDirectory
    val dbCreator = new DbAdapterImpl(dbDir)
    dbCreator.createTable(tableName, cols, pk, Some(idCol))
    dbCreator.insertData(tableName, Seq(idCol.valueOf(1), ageCol.valueOf(10), nameCOl.valueOf("abc"), activCol.valueOf(true)))
    dbCreator.insertData(tableName, Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCOl.valueOf("romain"), activCol.valueOf(false)))
    val data: Seq[LineValue] = dbCreator.getAll(tableName)
    val expected = Seq(LineValue(
      Seq(idCol.valueOf(1), ageCol.valueOf(10), nameCOl.valueOf("abc"), activCol.valueOf(true)),
    ),
      LineValue(
        Seq(idCol.valueOf(300), ageCol.valueOf(11), nameCOl.valueOf("romain"), activCol.valueOf(false)))
    )
    assertEquals(data, expected)
  }
}
