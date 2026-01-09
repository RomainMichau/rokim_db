package com.rokim.db.adapters.fs

import com.rokim.db.Domain.{AutoInc, Column, Index}
import com.rokim.db.adapters.fs.JsonCodecs.{*, given}
import io.circe.parser.*
import io.circe.syntax.*

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

case class TableMetadata(name: String, columns: Seq[Column[?]], pk: Seq[Column[?]], autoInc: Option[AutoInc[?]], indexes: List[Index]) {
  def addIndex(index: Index): TableMetadata = this.copy(indexes = index :: indexes)
  def getTablePath(dbPath: Path): Path = dbPath.resolve(name)
}

object TableMetadata {
  def write(tableDirectory: Path, metadata: TableMetadata): Unit = {
    val json = metadata.asJson.spaces2
    Files.write(tableDirectory.resolve("metadata"), json.getBytes(StandardCharsets.UTF_8))
  }

  def read(tableDirectory: Path): TableMetadata = {
    val jsonString = new String(Files.readAllBytes(tableDirectory.resolve("metadata")), StandardCharsets.UTF_8)
    decode[TableMetadata](jsonString) match {
      case Right(meta) => meta
      case Left(err) => throw new RuntimeException(s"Failed to parse metadata JSON: $err")
    }
  }
}