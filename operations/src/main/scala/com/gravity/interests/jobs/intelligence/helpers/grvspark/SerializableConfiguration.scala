package com.gravity.interests.jobs.intelligence.helpers.grvspark

import java.io._

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import scala.collection._
import scala.util.control.NonFatal

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable with KryoSerializable {
	@transient private lazy val log = LoggerFactory.getLogger(getClass)

	private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
		out.defaultWriteObject()
		value.write(out)
	}

	private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
		value = new Configuration(false)
		value.readFields(in)
	}

	private def tryOrIOException[T](block: => T): T = {
		try {
			block
		} catch {
			case e: IOException =>
				log.error("Exception encountered", e)
				throw e
			case NonFatal(e) =>
				log.error("Exception encountered", e)
				throw new IOException(e)
		}
	}

	def write(kryo: Kryo, out: Output): Unit = {
		val dos = new DataOutputStream(out)
		value.write(dos)
		dos.flush()
	}

	def read(kryo: Kryo, in: Input): Unit = {
		value = new Configuration(false)
		value.readFields(new DataInputStream(in))
	}
}
