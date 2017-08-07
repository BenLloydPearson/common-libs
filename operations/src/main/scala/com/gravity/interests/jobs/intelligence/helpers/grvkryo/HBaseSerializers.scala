package com.gravity.interests.jobs.intelligence.helpers.grvkryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.gravity.hbase.schema.{HRow, HbaseTable, PrimitiveInputStream, PrimitiveOutputStream}

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

trait HBaseSerializers extends ByteConverterSerializers {

	def HRowKryoSerializer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]): Serializer[RR] = new Serializer[RR] {
	  override def write(kryo: Kryo, output: Output, value: RR): Unit = {
	    new PrimitiveOutputStream(output).writeRow(table, value)
	  }

	  override def read(kryo: Kryo, input: Input, clazz: Class[RR]): RR = {
	    new PrimitiveInputStream(input).readRow(table)
	  }
	}

}

