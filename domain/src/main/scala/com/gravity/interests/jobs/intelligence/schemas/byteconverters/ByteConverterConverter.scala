package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import org.apache.commons.codec.binary.Base64

object ByteConverterConverter {
  /**
   * Encodes any ComplexByteConverter-serializable object as a Base64 String.
   *
   * @param obj The object to be encoded to Base64.
   * @tparam T The type of the object.
   * @return A Base64-encoded representation of obj, decodable by fromBase64[T].
   */
  def toBase64[T : ComplexByteConverter](obj: T) : String = {
    val byteOutStream = new ByteArrayOutputStream()
    val primOutStream = new PrimitiveOutputStream(byteOutStream)

    try {
      primOutStream.writeObj(obj)
      primOutStream.flush()

      new String(Base64.encodeBase64(byteOutStream.toByteArray))
    } finally {
      primOutStream.close()
    }
  }

  /**
   * Decodes (a.k.a. de-serializes) a Base64 String created by toBase64[T] into an object of type T.
   *
   * @param base64 The Base64 String created by toBase64[T].
   * @tparam T The type of the objec to be de-serialized.
   * @return The de-serialized object.
   */
  def fromBase64[T : ComplexByteConverter](base64: String) : T = {
    val byteArray = Base64.decodeBase64(base64)

    val primInputStream = new PrimitiveInputStream(new ByteArrayInputStream(byteArray))

    try {
      primInputStream.readObj[T]
    } finally {
      primInputStream.close()
    }
  }
}
