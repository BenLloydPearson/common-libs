package com.gravity.test

import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayInputStream, ByteArrayOutputStream}

import com.gravity.hbase.schema.{PrimitiveInputStream, PrimitiveOutputStream, ComplexByteConverter}

/**
 * Created by tchappell on 8/29/14.
 */
trait SerializationTesting {
  // Use Java Serialization to convert a Serializable object to an Array of bytes.
  def javaSerObjToBytes[T](obj: T) : Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)

    try {
      oos.writeObject(obj)
      oos.flush()

      bos.toByteArray
    } finally {
      if (oos != null)
        oos.close()
    }
  }

  // Use Java Serialization to reconstitute a previously-serialized object from an Array of bytes.
  def javaSerBytesToObj[T](bytes: Array[Byte]) : T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)

    try {
      ois.readObject().asInstanceOf[T]
    } finally {
      if (ois != null)
        ois.close()
    }
  }

  // Use Java Serialization to make a deep clone of a Serializable object.
  def deepCloneWithJavaSerialization[T](obj: T) : T = {
    javaSerBytesToObj[T]( javaSerObjToBytes(obj) )
  }

  /**
   * Exercises ComplexByteConverter[T] round-trip to return a deep clone of `orig`.
   */
  def deepCloneWithComplexByteConverter[T](orig: T)(implicit c : com.gravity.hbase.schema.ComplexByteConverter[T]): T = {
    val byteOutStream = new ByteArrayOutputStream()
    val primOutStream = new PrimitiveOutputStream(byteOutStream)

    try {
      primOutStream.writeObj(orig)
      primOutStream.flush()

      val primInputStream = new PrimitiveInputStream(new ByteArrayInputStream(byteOutStream.toByteArray()))

      try {
        val copy = primInputStream.readObj[T]

        //        if (orig == copy)
        //          println(s"orig=copy=`$copy`")
        //        else
        //          println(s"`$orig` is NOT EQUAL to `$copy`")

        copy
      } finally {
        primInputStream.close()
      }
    } finally {
      primOutStream.close()
    }
  }
}
