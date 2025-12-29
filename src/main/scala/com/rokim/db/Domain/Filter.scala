package com.rokim.db.Domain

sealed trait Comparator[U] {
  def compare(u1: U, u2 :U): Boolean
}

case class Equals[U]() extends Comparator[U] {
  override def compare(u1: U, u2: U): Boolean = u1 == u2
}

trait GreaterOperator[U] {
  def >(u1: U, u2: U): Boolean
}
given GreaterOperator[Int] with {
  override def >(u1: Int, u2: Int): Boolean = u1 > u2
}
given GreaterOperator[Long] with {
  override def >(u1: Long, u2: Long): Boolean = u1 > u2
}

case class Greater[U]()(using greaterThan: GreaterOperator[U]) extends Comparator[U] {
  override def compare(u1: U, u2: U): Boolean = greaterThan.>(u1, u2)
}

trait SmallerOperator[U] {
  def <(u1: U, u2: U): Boolean
}
given SmallerOperator[Int] with {
  override def <(u1: Int, u2: Int): Boolean = u1 < u2
}
given SmallerOperator[Long] with {
  override def <(u1: Long, u2: Long): Boolean = u1 < u2
}


case class Smaller[U]()(using smallerOperator: SmallerOperator[U])  extends Comparator[U] {
  override def compare(u1: U, u2: U): Boolean = smallerOperator.<(u1, u2)
}

case class Filter[U](column: Column[U], comparator: Comparator[U], comparedTo: U)