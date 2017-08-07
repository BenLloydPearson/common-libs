package com.gravity.interests.jobs.intelligence.helpers.grvkryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.gravity.hbase.schema._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait ByteConverterSerializers extends DefaultKryoSerializer {

	implicit def byteConverterSerializer[T](implicit converter: ByteConverter[T]): Serializer[T] = converter match {

		case c: ComplexByteConverter[T] => new Serializer[T] {
			override def write(kryo: Kryo, output: Output, value: T): Unit = c.write(value, new PrimitiveOutputStream(output))
			override def read(kryo: Kryo, input: Input, clazz: Class[T]): T = c.read(new PrimitiveInputStream(input))
		}

		case c: ByteConverter[T] => new Serializer[T] {
			override def write(kryo: Kryo, output: Output, value: T): Unit = {
				val bytes = converter.toBytes(value)
				output.writeInt(bytes.length, true)
				output.write(bytes)
			}

			override def read(kryo: Kryo, input: Input, clazz: Class[T]): T = {
				val len = input.readInt(true)
				converter.fromBytes(input.readBytes(len))
			}
		}
	}

}
