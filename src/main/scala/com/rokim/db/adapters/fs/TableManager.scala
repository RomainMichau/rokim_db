package com.rokim.db.adapters.fs

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.rokim.db.Domain.{Column, ColumnValue, Filter, Index, LineValue}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.reflect.ClassTag

class TableManager(tableDirectory: Path) {
  private val metadata = TableMetadata.read(tableDirectory)
  private val dataFile = tableDirectory.resolve("data")

  // Create BpTreeManager instances for each index - now simple with type-erased approach!
  val indexesManager: Map[Index, BpTreeManager] = metadata.indexes.map { index =>
    index -> new BpTreeManager(tableDirectory, metadata, index)
  }.toMap

  def insertData(value: Seq[ColumnValue[?]]): Unit = {
    val bytes = value.flatMap(_.value.toBytes).toArray
    Files.write(
      dataFile,
      bytes,
      StandardOpenOption.APPEND,
      StandardOpenOption.CREATE
    )
  }

  private def getTableLines: fs2.Stream[IO, LineValue] = {
    val cols = metadata.columns
    val lineByteCount = cols.map(_.colType.byteSize).sum
    fs2.io.file.Files[IO].readAll(fs2.io.file.Path.fromNioPath(dataFile))
      .chunkN(lineByteCount, allowFewer = false)
      .map { chunk =>
        LineValue.fromBytes(cols, chunk.asSeq)
      }
  }

  def getAll: Seq[LineValue] = {
    getTableLines.compile.toList.unsafeRunSync()
  }

  def getFiltered(filter: Filter): Seq[LineValue] = {
    getTableLines
      .filter(filter.apply)
      .compile
      .toList
      .unsafeRunSync()
  }

  /**
   * Get data from an index.
   */
  def getFromIndex[U](index: Index): ColumnValue[U] = {
    ???
  }

  /**
   * Create an index for this table.
   */
  def createIndex(index: Index): Unit = {
    ???
  }
}
