package org.cameron.cs.common

import org.apache.spark.internal.Logging

import java.nio.{ByteBuffer, ByteOrder}
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.UUID

/**
 * Utility object for generating keys using various hash algorithms.
 */
object KeyGenerator extends Logging {

  /**
   * Generates a key from the provided value using the specified hash algorithm.
   *
   * @param value         The input value for generating the key.
   * @param hashAlgorithm The hash algorithm to use for generating the key.
   * @return The generated key as a string.
   */
  def getKey(value: String, hashAlgorithm: String = "MD5"): String = {
    try {
      val hash: Array[Byte] = MessageDigest.getInstance(hashAlgorithm).digest(value.getBytes("UTF-8"))
      toUUID(hash).toString.replace("-", "")
    } catch {
      case _: NoSuchAlgorithmException =>
        logError(s"Error: Hash algorithm $hashAlgorithm not found.")
        ""
    }
  }

  /**
   * Converts the binary encoding to a UUID.
   *
   * @param binaryEncoding The binary encoding to convert to a UUID.
   * @return The UUID generated from the binary encoding.
   */
  private def toUUID(binaryEncoding: Array[Byte]): UUID = {
    val source: ByteBuffer = ByteBuffer.wrap(binaryEncoding)
    val target: ByteBuffer = ByteBuffer
      .allocate(16)
      .order(ByteOrder.BIG_ENDIAN)
      .putInt(source.getInt)
      .putShort(source.getShort)
      .putShort(source.getShort)
      .putLong(source.getLong)
    target.flip()
    new UUID(target.getLong, target.getLong)
  }
}