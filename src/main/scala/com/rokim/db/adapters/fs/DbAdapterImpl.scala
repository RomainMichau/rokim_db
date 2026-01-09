package com.rokim.db.adapters.fs

import com.rokim.db.Domain.{AutoInc, Column, ColumnValue, DbAdapter, Filter, Index, LineValue}

import java.nio.file.{Files, Path}
import scala.collection.mutable

class DbAdapterImpl(dbPath: Path) extends DbAdapter {
  Files.createDirectories(dbPath)

  private val tableManagers = mutable.Map[String, TableManager]()

  /**
   * Get or create a TableManager for the given table name.
   */
  private def getTableManager(tableName: String): TableManager = {
    tableManagers.getOrElseUpdate(tableName, {
      val tableDirectory = dbPath.resolve(tableName)
      new TableManager(tableDirectory)
    })
  }

  override def createTable(tableName: String, columns: Seq[Column[?]], pk: Seq[Column[?]], autoInc: Option[Column[?]]): Unit = {
    val tableDirectory = dbPath.resolve(tableName)
    Files.createDirectories(tableDirectory)
    TableMetadata.write(tableDirectory, TableMetadata(tableName, columns, pk, autoInc.map(col => AutoInc(col, 0)), List.empty))
    Files.createFile(tableDirectory.resolve("data"))
  }

  override def insertData(tableName: String, value: Seq[ColumnValue[?]]): Unit = {
    getTableManager(tableName).insertData(value)
  }

  override def getAll(tableName: String): Seq[LineValue] = {
    getTableManager(tableName).getAll
  }

  override def getFiltered(tableName: String, filter: Filter): Seq[LineValue] = {
    getTableManager(tableName).getFiltered(filter)
  }

  override def createIndex(index: Index): Unit = ???
}
