package com.rokim.db.Domain

object DbAdapter {
}
trait DbAdapter {
  def createTable(tableName: String, columns: Seq[Column[?]], pk: Seq[Column[?]], autoInc: Option[Column[?]]): Unit
  def insertData(tableName: String, value: Seq[ColumnValue[?]]): Unit
  def getAll(tableName: String): Seq[LineValue]
  def getFiltered(tableName: String, filter: Filter): Seq[LineValue]
  def createIndex(index: Index): Unit
}
