package com.rokim.db.Domain

import com.rokim.db.Utils

import scala.reflect.ClassTag

sealed trait Value[U] {
  def dbType: DbType[U]

  def value: U

  def toBytes: Array[Byte]
}

case class ValueInt(i: Int) extends Value[Int] {
  override def dbType: DbType[Int] = INT

  override def value: Int = i

  override def toBytes: Array[Byte] = Utils.intToBytes(i)
}

case class ValueLong(l: Long) extends Value[Long] {
  override def dbType: DbType[Long] = LONG

  override def value: Long = l

  override def toBytes: Array[Byte] = Utils.longToBytes(l)
}

case class ValueBoolean(b: Boolean) extends Value[Boolean] {
  override def dbType: DbType[Boolean] = BOOLEAN

  override def value: Boolean = b

  override def toBytes: Array[Byte] = Array(Utils.boolToByte(b))
}

case class ValueVarChar(str: String, t: VARCHAR) extends Value[String] {

  override def dbType: DbType[String] = t

  override def value: String = str

  override def toBytes: Array[Byte] = Utils.paddedStringToBytes(str, t.size)
}

/**
 *
 * @tparam U : U is the scala type used to represent the DB type
 */
sealed trait DbType[U] {
  def byteSize: Int

  def valueOf(u: U): Value[U]

  def valueOf(u: Seq[Byte]): Value[U]
}

case class VARCHAR(size: Int) extends DbType[String] {
  override def byteSize: Int = size

  override def valueOf(u: String): Value[String] = ValueVarChar(u, this)

  override def valueOf(u: Seq[Byte]): Value[String] = valueOf(Utils.bytesToPaddedString(u.toArray))
}

case object INT extends DbType[Int] {
  override def byteSize: Int = 4

  override def valueOf(u: Int): Value[Int] = ValueInt(u)

  override def valueOf(u: Seq[Byte]): Value[Int] = valueOf(Utils.bytesToInt(u.toArray))
}

case object LONG extends DbType[Long] {
  override def byteSize: Int = 8

  override def valueOf(u: Long): Value[Long] = ValueLong(u)

  override def valueOf(u: Seq[Byte]): Value[Long] = valueOf(Utils.bytesToLong(u.toArray))
}

case object BOOLEAN extends DbType[Boolean] {
  override def byteSize: Int = 1

  override def valueOf(u: Boolean): Value[Boolean] = ValueBoolean(u)

  override def valueOf(u: Seq[Byte]): Value[Boolean] = valueOf(Utils.byteToBool(u.head))
}


case class ColumnName(name: String)

object Column {
  def apply[U](name: String, colType: DbType[U]): Column[U] = Column(ColumnName(name), colType)
}

case class Column[U](name: ColumnName, colType: DbType[U]) {
  def valueOf(u: U): ColumnValue[U] = ColumnValue(name, colType.valueOf(u))

  def valueOf(u: Seq[Byte]): ColumnValue[U] = ColumnValue(name, colType.valueOf(u))
}

case class ColumnValue[U](name: ColumnName, value: Value[U])

object LineValue {
  def empty: LineValue = LineValue(Seq.empty)

  def fromBytes(cols: Seq[Column[?]], bytes: Seq[Byte]): LineValue = {
    cols match {
      case headCol :: tailCols => bytes.splitAt(headCol.colType.byteSize) match
        case (headBytes, tailBytes) => LineValue(headCol.valueOf(headBytes) +: fromBytes(tailCols, tailBytes).values)
      case Nil => LineValue.empty
    }
  }
}

case class LineValue(values: Seq[ColumnValue[?]]) {
  def getColumn(name: String): ColumnValue[?] = values.find(_.name.name == name).get
}

case class AutoInc[U](col: Column[U], count: Long) {
  override def toString: String = {
    s"AUTOINC ${col.name.name} count\n"
  }
}

case class Index(name: String, columns: Seq[Column[?]])