package com.gravity.interests.jobs.intelligence

import com.gravity.hbase.schema._

import scala.collection._


/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Generic byte converters that will be used if no high-priority implicit converter is found
 */
trait LowPrioritySchemaTypes {

  implicit def defaultOptionByteConverter[T](implicit bc: ComplexByteConverter[T]): ComplexByteConverter[Option[T]] = new ComplexByteConverter[Option[T]] {
    override def write(data: Option[T], output: PrimitiveOutputStream): Unit = data match {
      case Some(d) =>
        output.writeBoolean(true)
        output.writeObj[T](d)
      case None =>
        output.writeBoolean(false)
    }

    override def read(input: PrimitiveInputStream): Option[T] = {
      input.readBoolean() match {
        case true => Some(input.readObj[T])
        case false => None
      }
    }
  }

  implicit def defaultSeqByteConverter[T](implicit bc: ComplexByteConverter[T]): ComplexByteConverter[Seq[T]] = new ComplexByteConverter[Seq[T]] {
    override def write(data: scala.Seq[T], output: PrimitiveOutputStream): Unit = {
      output.writeInt(data.length)
      for (i <- data) output.writeObj[T](i)
    }

    override def read(input: PrimitiveInputStream): scala.Seq[T] = {
      val len = input.readInt()
      for (i <- 0 until len) yield input.readObj[T]
    }
  }

}
