package com.rokim.db

import java.nio.ByteBuffer

object Utils {
  def intToBytes(i: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(i).array()
  def shortToBytes(i: Short): Array[Byte] = ByteBuffer.allocate(2).putShort(i).array()
  def longToBytes(i: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(i).array()
  def boolToByte(i: Boolean): Byte = if(i) 1.byteValue else 0.byteValue
  def stringToBytes(str: String): Array[Byte] = str.getBytes("UTF8")
  def paddedStringToBytes(str: String, len: Int): Array[Byte] = str.getBytes("UTF8").padTo(len, 0.toByte)

  def bytesToShort(bytes: Array[Byte]): Short = ByteBuffer.wrap(bytes).getShort()
  def bytesToInt(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt()
  def bytesToLong(bytes: Array[Byte]): Long = ByteBuffer.wrap(bytes).getLong()
  def byteToBool(byte: Byte): Boolean = byte != 0
  def bytesToString(bytes: Array[Byte]): String = new String(bytes, "UTF8")
  def bytesToPaddedString(bytes: Array[Byte]): String = new String(bytes.takeWhile(_ != 0), "UTF8")
}
