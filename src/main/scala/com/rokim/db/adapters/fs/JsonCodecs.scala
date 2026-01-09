package com.rokim.db.adapters.fs

import com.rokim.db.Domain.{AutoInc, BOOLEAN, Column, ColumnName, DbType, INT, Index, LONG, VARCHAR}
import io.circe.*
import io.circe.generic.semiauto.*

object JsonCodecs {
  given Encoder[DbType[?]] = Encoder.instance {
    case INT => Json.obj("type" -> Json.fromString("INT"))
    case LONG => Json.obj("type" -> Json.fromString("LONG"))
    case BOOLEAN => Json.obj("type" -> Json.fromString("BOOLEAN"))
    case VARCHAR(size) => Json.obj("type" -> Json.fromString("VARCHAR"), "size" -> Json.fromInt(size))
  }

  given Decoder[DbType[?]] = Decoder.instance { cursor =>
    cursor.get[String]("type").flatMap {
      case "INT" => Right(INT)
      case "LONG" => Right(LONG)
      case "BOOLEAN" => Right(BOOLEAN)
      case "VARCHAR" => cursor.get[Int]("size").map(VARCHAR(_))
      case other => Left(DecodingFailure(s"Unknown DbType: $other", cursor.history))
    }
  }

  given Encoder[Column[?]] = Encoder.instance { col =>
    val colTypeJson = col.colType match {
      case INT => Json.obj("type" -> Json.fromString("INT"))
      case LONG => Json.obj("type" -> Json.fromString("LONG"))
      case BOOLEAN => Json.obj("type" -> Json.fromString("BOOLEAN"))
      case VARCHAR(size) => Json.obj("type" -> Json.fromString("VARCHAR"), "size" -> Json.fromInt(size))
    }
    Json.obj(
      "name" -> Json.fromString(col.name.name),
      "colType" -> colTypeJson
    )
  }

  given Decoder[Column[?]] = Decoder.instance { cursor =>
    for {
      name <- cursor.get[String]("name")
      colType <- cursor.get[DbType[?]]("colType")
    } yield Column(ColumnName(name), colType)
  }

  given Encoder[AutoInc[?]] = Encoder.instance { autoInc =>
    val colTypeJson = autoInc.col.colType match {
      case INT => Json.obj("type" -> Json.fromString("INT"))
      case LONG => Json.obj("type" -> Json.fromString("LONG"))
      case BOOLEAN => Json.obj("type" -> Json.fromString("BOOLEAN"))
      case VARCHAR(size) => Json.obj("type" -> Json.fromString("VARCHAR"), "size" -> Json.fromInt(size))
    }
    val columnJson = Json.obj(
      "name" -> Json.fromString(autoInc.col.name.name),
      "colType" -> colTypeJson
    )
    Json.obj(
      "column" -> columnJson,
      "count" -> Json.fromLong(autoInc.count)
    )
  }

  given Decoder[AutoInc[?]] = Decoder.instance { cursor =>
    for {
      col <- cursor.get[Column[?]]("column")
      count <- cursor.get[Long]("count")
    } yield AutoInc(col, count)
  }

  given Encoder[Index] = Encoder.instance { index =>
    Json.obj(
      "name" -> Json.fromString(index.name),
      "columns" -> Json.fromValues(index.columns.map(summon[Encoder[Column[?]]].apply))
    )
  }

  given Decoder[Index] = Decoder.instance { cursor =>
    for {
      name <- cursor.get[String]("name")
      columns <- cursor.get[Seq[Column[?]]]("columns")
    } yield Index(name, columns)
  }

  private def encodeColumn(col: Column[?]): Json = {
    val colTypeJson = col.colType match {
      case INT => Json.obj("type" -> Json.fromString("INT"))
      case LONG => Json.obj("type" -> Json.fromString("LONG"))
      case BOOLEAN => Json.obj("type" -> Json.fromString("BOOLEAN"))
      case VARCHAR(size) => Json.obj("type" -> Json.fromString("VARCHAR"), "size" -> Json.fromInt(size))
    }
    Json.obj(
      "name" -> Json.fromString(col.name.name),
      "colType" -> colTypeJson
    )
  }

  private def encodeAutoInc(autoInc: AutoInc[?]): Json = {
    val colTypeJson = autoInc.col.colType match {
      case INT => Json.obj("type" -> Json.fromString("INT"))
      case LONG => Json.obj("type" -> Json.fromString("LONG"))
      case BOOLEAN => Json.obj("type" -> Json.fromString("BOOLEAN"))
      case VARCHAR(size) => Json.obj("type" -> Json.fromString("VARCHAR"), "size" -> Json.fromInt(size))
    }
    val columnJson = Json.obj(
      "name" -> Json.fromString(autoInc.col.name.name),
      "colType" -> colTypeJson
    )
    Json.obj(
      "column" -> columnJson,
      "count" -> Json.fromLong(autoInc.count)
    )
  }

  given Encoder[TableMetadata] = Encoder.instance { metadata =>
    Json.obj(
      "name" -> Json.fromString(metadata.name),
      "columns" -> Json.fromValues(metadata.columns.map(encodeColumn)),
      "pk" -> Json.fromValues(metadata.pk.map(encodeColumn)),
      "autoInc" -> metadata.autoInc.map(encodeAutoInc).getOrElse(Json.Null),
      "indexes" -> Json.fromValues(metadata.indexes.map(summon[Encoder[Index]].apply))
    )
  }
  

  given Decoder[TableMetadata] = Decoder.instance { cursor =>
    for {
      name <- cursor.get[String]("name")
      columns <- cursor.get[Seq[Column[?]]]("columns")
      pk <- cursor.get[Seq[Column[?]]]("pk")
      autoInc <- cursor.get[Option[AutoInc[?]]]("autoInc")
      indexes <- cursor.get[List[Index]]("indexes")
    } yield TableMetadata(name, columns, pk, autoInc, indexes)
  }
}