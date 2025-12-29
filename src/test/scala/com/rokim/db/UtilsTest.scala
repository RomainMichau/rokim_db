package com.rokim.db

import java.nio.ByteBuffer

class UtilsTest extends munit.FunSuite {

  test("intToBytes converts positive integer correctly") {
    val result = Utils.intToBytes(12345)
    val expected = ByteBuffer.allocate(4).putInt(12345).array()
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 4)
  }

  test("intToBytes converts negative integer correctly") {
    val result = Utils.intToBytes(-12345)
    val expected = ByteBuffer.allocate(4).putInt(-12345).array()
    assertEquals(result.toList, expected.toList)
  }

  test("intToBytes converts zero correctly") {
    val result = Utils.intToBytes(0)
    val expected = Array[Byte](0, 0, 0, 0)
    assertEquals(result.toList, expected.toList)
  }

  test("intToBytes converts max integer correctly") {
    val result = Utils.intToBytes(Int.MaxValue)
    val expected = ByteBuffer.allocate(4).putInt(Int.MaxValue).array()
    assertEquals(result.toList, expected.toList)
  }

  test("longToBytes converts positive long correctly") {
    val result = Utils.longToBytes(123456789L)
    val expected = ByteBuffer.allocate(8).putLong(123456789L).array()
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 8)
  }

  test("longToBytes converts negative long correctly") {
    val result = Utils.longToBytes(-123456789L)
    val expected = ByteBuffer.allocate(8).putLong(-123456789L).array()
    assertEquals(result.toList, expected.toList)
  }

  test("longToBytes converts zero correctly") {
    val result = Utils.longToBytes(0L)
    val expected = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0)
    assertEquals(result.toList, expected.toList)
  }

  test("longToBytes converts max long correctly") {
    val result = Utils.longToBytes(Long.MaxValue)
    val expected = ByteBuffer.allocate(8).putLong(Long.MaxValue).array()
    assertEquals(result.toList, expected.toList)
  }

  test("boolToByte converts true to 1") {
    val result = Utils.boolToByte(true)
    assertEquals(result, 1.toByte)
  }

  test("boolToByte converts false to 0") {
    val result = Utils.boolToByte(false)
    assertEquals(result, 0.toByte)
  }

  test("stringToBytes converts ASCII string correctly") {
    val result = Utils.stringToBytes("hello")
    val expected = "hello".getBytes("UTF8")
    assertEquals(result.toList, expected.toList)
  }

  test("stringToBytes converts empty string correctly") {
    val result = Utils.stringToBytes("")
    val expected = Array[Byte]()
    assertEquals(result.toList, expected.toList)
  }

  test("stringToBytes converts unicode string correctly") {
    val result = Utils.stringToBytes("héllo 世界")
    val expected = "héllo 世界".getBytes("UTF8")
    assertEquals(result.toList, expected.toList)
  }

  test("paddedStringToBytes pads short string with zeros") {
    val result = Utils.paddedStringToBytes("abc", 8)
    val expected = Array[Byte]('a', 'b', 'c', 0, 0, 0, 0, 0)
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 8)
  }

  test("paddedStringToBytes handles exact length string") {
    val result = Utils.paddedStringToBytes("abcdefgh", 8)
    val expected = "abcdefgh".getBytes("UTF8")
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 8)
  }

  test("paddedStringToBytes does not truncate longer string") {
    val result = Utils.paddedStringToBytes("abcdefghijk", 8)
    val expected = "abcdefghijk".getBytes("UTF8")
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 11)
  }

  test("paddedStringToBytes handles empty string") {
    val result = Utils.paddedStringToBytes("", 5)
    val expected = Array[Byte](0, 0, 0, 0, 0)
    assertEquals(result.toList, expected.toList)
    assertEquals(result.length, 5)
  }

  test("paddedStringToBytes handles unicode with padding") {
    val result = Utils.paddedStringToBytes("é", 5)
    val unicodeBytes = "é".getBytes("UTF8")
    val expected = unicodeBytes.padTo(5, 0.toByte)
    assertEquals(result.toList, expected.toList)
  }
}