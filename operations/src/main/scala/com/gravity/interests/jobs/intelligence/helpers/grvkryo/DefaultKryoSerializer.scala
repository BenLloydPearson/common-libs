package com.gravity.interests.jobs.intelligence.helpers.grvkryo

import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.collection._
import scala.reflect.ClassTag

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait DefaultKryoSerializer {
	implicit def defaultKryoSerializer[T](implicit kryo: Kryo, ct: ClassTag[T]): Serializer[T] = {
		Option(kryo.getRegistration(ct.runtimeClass)).map(_.getSerializer).getOrElse(kryo.getDefaultSerializer(ct.runtimeClass)).asInstanceOf[Serializer[T]]
	}
}
