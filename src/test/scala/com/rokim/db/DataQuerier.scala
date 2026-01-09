package com.rokim.db

import com.rokim.db.Domain.{BooleanColumnFilter, Equals, Greater, IntColumnFilter, LineValue, given_GreaterOperator_Int}
import com.rokim.db.adapters.fs.DbAdapterImpl

import java.nio.file.Paths

object DataQuerier {

  def main(args: Array[String]): Unit = {
    // Configuration
    val dbPath = if (args.length > 0) args(0) else "test_dbs"
    val tableName = if (args.length > 1) args(1) else "people"

    println(s"Querying database: $dbPath/$tableName")
    println("=" * 60)

    val dbDirectory = Paths.get(dbPath)
    val adapter = new DbAdapterImpl(dbDirectory)

    // Get all data
    val allData = adapter.getAll(tableName)
    println(s"\nTotal entries: ${allData.size}")

    if (allData.isEmpty) {
      println("No data found in the table.")
      return
    }

    // Display first 10 entries
    println("\n--- First 10 entries ---")
    allData.take(10).foreach(printLineValue)

    // Display last 5 entries
    if (allData.size > 10) {
      println("\n--- Last 5 entries ---")
      allData.takeRight(5).foreach(printLineValue)
    }

    // Query with filter: age > 30
    println("\n--- Filtering: age > 30 ---")
    val ageFilter = IntColumnFilter("age", Greater[Int](), 30)
    val filtered = adapter.getFiltered(tableName, ageFilter)
    println(s"Found ${filtered.size} entries with age > 30")
    filtered.take(5).foreach(printLineValue)

    // Query with filter: age > 50
    println("\n--- Filtering: age > 50 ---")
    val ageFilter50 = IntColumnFilter("age", Greater[Int](), 50)
    val filtered50 = adapter.getFiltered(tableName, ageFilter50)
    println(s"Found ${filtered50.size} entries with age > 50")
    filtered50.take(5).foreach(printLineValue)

    println("\n--- Active only ---")
    val activeFilter =  BooleanColumnFilter("active", Equals(), true)
    val filteredActvice = adapter.getFiltered(tableName, activeFilter)
    println(s"Found ${filteredActvice.size} entries with age > 50")
    filteredActvice.take(5).foreach(printLineValue)



  }

  private def printLineValue(line: LineValue): Unit = {
    val id = line.getColumn("id").value
    val age = line.getColumn("age").value
    val name = line.getColumn("name").value
    val active = line.getColumn("active").value
    println(f"  ID: $id%-6s | Age: $age%-3s | Name: $name%-20s | Active: $active")
  }
}
