package com.rokim.db.adapters.fs

import com.rokim.db.Domain.Index
import com.rokim.db.Utils

import java.nio.file.{Files, Path}
import scala.reflect.ClassTag

trait Byter[I] {
  def apply(i: I): Array[Byte]
}
object BpTreeManager {
  val pageSize = 4096


  type Pointer = Int

  object Header {
    def root: Header = Header(false, 0, 0)
  }

  // 6 bytes
  case class Header(isLeaf: Boolean, numberOfKeys: Short, pageNumber: Pointer) {
    def toByte: Array[Byte] = Array(Utils.boolToByte(isLeaf)) ++ Utils.shortToBytes(numberOfKeys) ++ Utils.intToBytes(pageNumber)
  }

  object InternalNode {
    def root[K: ClassTag]: InternalNode[K] = InternalNode(Header.root, Array.empty[K], Array.empty[Pointer])
  }

  case class InternalNode[K](header: Header, keys: Array[K], pointers: Array[Pointer]) {
    def toBytes(using byter: Byter[K]): Array[Byte] = {
      val bytes = header.toByte ++ keys.flatMap(byter.apply) ++ pointers.flatMap(Utils.intToBytes)
      bytes.padTo(pageSize, 0)
    }
  }

  case class LeafNode[K, V](header: Header, keys: Array[K], values: Array[V], nextLeaf: Pointer) {
    def toByte(using byterK: Byter[K], byterVal: Byter[V]): Array[Byte] = {
      val bytes = header.toByte ++ keys.flatMap(byterK.apply) ++ values.flatMap(byterVal.apply) ++ Utils.intToBytes(nextLeaf)
      bytes.padTo(pageSize, 0)
    }
  }

}

// B+tree
// https://medium.com/@akashsdas_dev/b-trees-and-b-trees-682d363df1f7
//
// On-Disk Structure:
// ==================
//
// The B+tree is stored as a sequence of fixed-size pages (4096 bytes each).
// Each page is either an internal node or a leaf node.
//
// Page Header (common to all node types):
// ----------------------------------------
// Offset | Size | Description
// -------|------|-------------
// 0      | 1    | Node type (0 = leaf, 1 = internal)
// 1      | 2    | Number of keys in this node
// 3      | 8    | Page number (offset in file / 4096)
// 11     | ...  | Node-specific data
//
// Internal Node Structure:
// ------------------------
// [Header (11 bytes)]
// [Child₀ pointer (8 bytes)] [Key₀ (variable)]
// [Child₁ pointer (8 bytes)] [Key₁ (variable)]
// ...
// [Childₙ pointer (8 bytes)] [Keyₙ (variable)]
// [Childₙ₊₁ pointer (8 bytes)]  // rightmost child
//
// - Keys are stored in sorted order
// - Child₀ points to children with keys < Key₀
// - Child₁ points to children with keys >= Key₀ and < Key₁
// - Childᵢ points to children with keys >= Keyᵢ₋₁ and < Keyᵢ
// - Childₙ₊₁ points to children with keys >= Keyₙ
// - Child pointers are 8-byte page numbers
// - Key size depends on the indexed column types
//
// Leaf Node Structure:
// --------------------
// [Header (11 bytes)]
// [Key₀ (variable)] [Value₀ pointer (8 bytes)]
// [Key₁ (variable)] [Value₁ pointer (8 bytes)]
// ...
// [Keyₙ (variable)] [Valueₙ pointer (8 bytes)]
// [Next leaf pointer (8 bytes)]  // pointer to next leaf node for range scans
//
// - Keys are stored in sorted order
// - Each key is followed by a pointer to the data row (byte offset in data file)
// - Leaf nodes are linked together for efficient range queries
// - Value pointers are 8-byte offsets into the table's data file
//
// File Layout:
// ------------
// [Page 0: Root node]
// [Page 1: Internal/Leaf node]
// [Page 2: Internal/Leaf node]
// ...
// [Page n: Internal/Leaf node]
//
// The root is always at page 0. As the tree grows, pages are allocated sequentially.
class BpTreeManager(tablePath: Path, tableMetadata: TableMetadata, index: Index) {
  private val indexPath = tablePath.resolve(s"idx_${index.name}")

  /**
   * Serialize a key value based on the column types in the index.
   */
  private def serializeKey(values: Seq[Any]): Array[Byte] = {
    index.columns.zip(values).flatMap { case (col, value) =>
      col.colType match {
        case com.rokim.db.Domain.INT => Utils.intToBytes(value.asInstanceOf[Int])
        case com.rokim.db.Domain.LONG => Utils.longToBytes(value.asInstanceOf[Long])
        case com.rokim.db.Domain.BOOLEAN => Array(Utils.boolToByte(value.asInstanceOf[Boolean]))
        case com.rokim.db.Domain.VARCHAR(size) => Utils.paddedStringToBytes(value.asInstanceOf[String], size)
      }
    }.toArray
  }

  /**
   * Create an empty root node and write it to disk.
   * The root starts as an empty leaf node.
   */
  def create(): Unit = {
    val rootHeader = BpTreeManager.Header(
      isLeaf = true,        // Start as a leaf node
      numberOfKeys = 0,     // No keys initially
      pageNumber = 0 // Root is always at page 0
    )

    // Serialize the header and pad to page size
    val rootNodeBytes = rootHeader.toByte.padTo(BpTreeManager.pageSize, 0.toByte)

    Files.write(indexPath, rootNodeBytes)
  }
}
