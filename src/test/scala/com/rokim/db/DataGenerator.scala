package com.rokim.db

import com.rokim.db.Domain.*
import com.rokim.db.adapters.fs.DbAdapterImpl

import java.nio.file.{Files, Paths}
import scala.util.{Random, Try}

object DataGenerator {

  def main(args: Array[String]): Unit = {
    // Configuration
    val N = if (args.length > 0) args(0).toInt else 1000000
    val dbPath = if (args.length > 1) args(1) else "test_dbs"
    val tableName = if (args.length > 2) args(2) else "people"

    println(s"Generating $N entries in $dbPath/$tableName...")

    // Create db directory if it doesn't exist
    val dbDirectory = Paths.get(dbPath)
    if (!Files.exists(dbDirectory)) {
      Files.createDirectories(dbDirectory)
      println(s"Created directory: $dbDirectory")
    }

    // Define table schema
    val idCol = Column("id", LONG)
    val ageCol = Column("age", INT)
    val nameCol = Column("name", VARCHAR(50))
    val activeCol = Column("active", BOOLEAN)
    val cols = Seq(idCol, ageCol, nameCol, activeCol)
    val pk = Seq(idCol)

    // Create adapter and table
    val adapter = new DbAdapterImpl(dbDirectory)
    Try {
      adapter.createTable(tableName, cols, pk, Some(idCol))
    }
    println(s"Created table: $tableName")

    // Name lists for variety
    val firstNames = Seq("Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry",
                          "Iris", "Jack", "Kate", "Liam", "Mary", "Noah", "Olivia", "Peter")
    val lastNames = Seq("Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                         "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor")

    val random = new Random()

    // Generate N entries
    (1 to N).foreach { i =>
      val firstName = firstNames(random.nextInt(firstNames.length))
      val lastName = lastNames(random.nextInt(lastNames.length))
      val age = 18 + random.nextInt(65) // Ages between 18 and 82
      val active = random.nextBoolean()

      adapter.insertData(
        tableName,
        Seq(
          idCol.valueOf(i.toLong),
          ageCol.valueOf(age),
          nameCol.valueOf(s"$firstName $lastName"),
          activeCol.valueOf(active)
        )
      )

      if (i % 100 == 0) {
        print(s"\rInserted $i entries...")
      }
    }

    println(s"\n✓ Successfully generated $N entries in $dbPath/$tableName")

    // Display some sample data
    val allData = adapter.getAll(tableName)
    println(s"\nTotal entries: ${allData.size}")
    println(s"First entry: ${allData.head}")
    println(s"Last entry: ${allData.last}")
  }
}