package com.rokim.db.adapters.fs

import DbAdapterImpl.{TableMetadata, columnFromString}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.rokim.db.Domain.{AutoInc, BOOLEAN, Column, ColumnName, ColumnValue, DbAdapter, INT, LONG, LineValue, VARCHAR, Value}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters.given

object DbAdapterImpl {


  private def columnToString[U](col: Column[U]): String = {
    val typeSt = col.colType match {
      case INT => "INT"
      case LONG => "LONG"
      case BOOLEAN => "BOOLEAN"
      case VARCHAR(size) => s"VARCHAR $size"
    }
    s"${col.name.name} $typeSt"
  }

  private def columnFromString(st: String): Column[?] = {
    st.split(" ").toList match
      case name :: "INT" :: Nil => Column(ColumnName(name), INT)
      case name :: "LONG" :: Nil => Column(ColumnName(name), LONG)
      case name :: "BOOLEAN" :: Nil => Column(ColumnName(name), BOOLEAN)
      case name :: "VARCHAR" :: size :: Nil => Column(ColumnName(name), VARCHAR(size.toInt))
  }

  case class TableMetadata(name: String, columns: Seq[Column[?]], pk: Seq[Column[?]], autoInc: Option[AutoInc[?]]) {
    override def toString: String = {
      val colSt = columns.map(columnToString).mkString("\n")
      val pkSt = pk.map(_.name.name).mkString(" ")
      val autoIncSt = autoInc.map { case AutoInc(col, count) => s"AUTOINC ${col.name.name} $count" }.getOrElse("")
      s"""$name
         |$autoIncSt
         |$pkSt
         |$colSt
         |""".stripMargin
    }
  }
}

class DbAdapterImpl(dbsPath: Path) extends DbAdapter {
  Files.createDirectories(dbsPath)


  private[fs] def writeTableMetadata(tableDirectory: Path, metadata: TableMetadata) = {
    Files.writeString(tableDirectory.resolve("metadata"), metadata.toString)
  }

  private[fs] def readTableMetadata(tableDirectory: Path): TableMetadata = {
    val metadata = Files.readAllLines(tableDirectory.resolve("metadata")).asScala.toList
    val dbName = metadata.head
    val autoIncSt = metadata(1)
    val autoIncColNameAndCount = autoIncSt.split(" ").toList match
      case Nil => None
      case "AUTOINC" :: col :: count :: Nil => Some((ColumnName(col), count.toLong))
    val pkName = metadata(2).split(" ").map(ColumnName.apply)
    val cols = metadata.drop(3).map(columnFromString)
    val autoInc = autoIncColNameAndCount.map { case (name, count) => AutoInc(cols.find(_.name == name).get, count) }
    val pk = pkName.map { name => cols.find(_.name == name).get }
    TableMetadata(dbName, cols, pk, autoInc)
  }

  private def datafile(tableDirectory: Path) = tableDirectory.resolve("data")

  override def createTable(tableName: String, columns: Seq[Column[?]], pk: Seq[Column[?]], autoInc: Option[Column[?]]): Unit = {
    val tableDirectory = dbsPath.resolve(tableName)
    Files.createDirectories(tableDirectory)
    writeTableMetadata(tableDirectory, TableMetadata(tableName, columns, pk, autoInc.map(col => AutoInc(col, 0))))
    Files.createFile(datafile(tableDirectory))

  }

  override def insertData(tableName: String, value: Seq[ColumnValue[?]]): Unit = {
    val tableDirectory = dbsPath.resolve(tableName)
    val bytes = value.flatMap(_.value.toBytes).toArray
    Files.write(
      datafile(tableDirectory),
      bytes,
      StandardOpenOption.APPEND,
      StandardOpenOption.CREATE
    )
  }

    override def getAll(tableName: String): Seq[LineValue] = {
      val tableDirectory = dbsPath.resolve(tableName)
      val cols = readTableMetadata(tableDirectory).columns
      val lineByteCount = cols.map(_.colType.byteSize).sum
      fs2.io.file.Files[IO].readAll(fs2.io.file.Path.fromNioPath(datafile(tableDirectory)))
        .chunkN(lineByteCount, allowFewer = false)
        .map { chunk =>
              LineValue.fromBytes(cols, chunk.asSeq)
        }.compile.toList.unsafeRunSync()
    }
}
