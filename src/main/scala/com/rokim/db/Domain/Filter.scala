package com.rokim.db.Domain

sealed trait Comparator[U] {
  def compare(u1: U, u2: U): Boolean
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


case class Smaller[U]()(using smallerOperator: SmallerOperator[U]) extends Comparator[U] {
  override def compare(u1: U, u2: U): Boolean = smallerOperator.<(u1, u2)
}

trait Filter {
  def apply(line: LineValue): Boolean

  def or(that: Filter): Filter = {
    line => this(line) || that.apply(line)
  }

  def and(that: Filter): Filter = {
    line => this(line) && that.apply(line)
  }
}

case class IntColumnFilter(columnName: String, comparator: Comparator[Int], expectedValue: Int) extends Filter {
  override def apply(line: LineValue): Boolean = {
    line.values.find(_.name.name == columnName).exists {
      case ColumnValue(_, ValueInt(actualValue)) => comparator.compare(actualValue, expectedValue)
      case _ => false
    }
  }
}

case class LongColumnFilter(columnName: String, comparator: Comparator[Long], expectedValue: Long) extends Filter {
  override def apply(line: LineValue): Boolean = {
    line.values.find(_.name.name == columnName).exists {
      case ColumnValue(_, ValueLong(actualValue)) => comparator.compare(actualValue, expectedValue)
      case _ => false
    }
  }
}

case class StringColumnFilter(columnName: String, comparator: Comparator[String], expectedValue: String) extends Filter {
  override def apply(line: LineValue): Boolean = {
    line.values.find(_.name.name == columnName).exists {
      case ColumnValue(_, ValueVarChar(actualValue, _)) => comparator.compare(actualValue, expectedValue)
      case _ => false
    }
  }
}

case class BooleanColumnFilter(columnName: String, comparator: Comparator[Boolean], expectedValue: Boolean) extends Filter {
  override def apply(line: LineValue): Boolean = {
    line.values.find(_.name.name == columnName).exists {
      case ColumnValue(_, ValueBoolean(actualValue)) => comparator.compare(actualValue, expectedValue)
      case _ => false
    }
  }
}

case class PredicateFilter(columnName: String, predicate: ColumnValue[?] => Boolean) extends Filter {
  override def apply(line: LineValue): Boolean = {
    line.values.find(_.name.name == columnName).exists(predicate)
  }
}

