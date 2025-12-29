package com.rokim.db

import com.rokim.db.Domain.{Column, ColumnName, LONG, VARCHAR}
import com.rokim.db.adapters.fs.DbAdapterImpl

import java.nio.file.Paths

object Main {
  def main(args: Array[String]): Unit = {
    val dbCreator = new DbAdapterImpl(Paths.get("./test_dbs"))
//    dbCreator.createDb("test1", Seq(Column(ColumnName("id"), LONG), Column(ColumnName("name"), VARCHAR(100))), Seq(ColumnName("id")), None)
  }
}