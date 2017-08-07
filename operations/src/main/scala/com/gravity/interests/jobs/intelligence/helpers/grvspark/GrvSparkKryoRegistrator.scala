package com.gravity.interests.jobs.intelligence.helpers.grvspark

import java.io.{DataInputStream, DataOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoException, Registration, Serializer}
import com.gravity.domain.ontology.{EdgeInfo, EnrichedOntologyNode, NodeInfo, TraversalInfo}
import com.gravity.hbase.schema.{DeleteOp, IncrementOp, OpsResult, PutOp}
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.utilities.{ClassName}
import com.gravity.interests.jobs.intelligence.helpers.grvkryo._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.spark.graphx.Edge
import org.apache.spark.serializer.KryoRegistrator
import org.joda.time.{Chronology, DateTime, DateTimeZone, LocalTime}
import org.openrdf.model.Statement

import scala.collection._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 *
 * This class allows us to call out explicit registrations for optimization purposes.  When classes are
 * registered, their classname doesn't need to be serialized with the data.
 */


class GrvSparkKryoRegistrator extends GrvKryoRegistrationFunctions with KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {

    // pull in implicit conversions from existing gravity serializers
    implicit val _kryo = kryo

    // at some point we could consider changing this to true which would require us to test all SparkApp's
    // to make sure that all types are registered
    kryo.setRegistrationRequired(false)

    /*
     * Always APPEND registrations in each group below, since order of registration could affect backward compatibility
     * The 'registerGroup' is a way to allow us to keep registrations organized into groups which can be appended
     * to independently rather than maintaining a single list of registrations which could get messy as
     * more and more registrations are added.
     *
     * So if we create a Group A of registrations with size 100, we can register up to 100 types for that
     * group while still making sure that Group B registrations always start at 101.
     *
     * 'register[T]' will look for and implicit value of Serializer[T]
     * 'registerCustom[T]' will use the user-defined methods for serialization
     */

    /*
      Unfortunately there's no easy way to order tables based on creation time so hopefully
      tables are *appended* in the Schema rather than inserted.
     */
    registerGroup("Schema Types", 200)(

      Schema.getTableReferences.flatMap(entry => Seq(
        register(entry.keyClass, entry.kryoKeySerializer),
        register(entry.rowClass, entry.kryoRowSerializer)
      )): _*
    )

    registerGroup("HPaste Types", 25)(
      register[OpsResult],
      register[PutOp[_, _]],
      register[DeleteOp[_, _]],
      register[IncrementOp[_, _]]
    )

    registerGroup("Joda Types", 20)(
      registerCustom[DateTime]((dt, out) => out.writeLong(dt.getMillis, true), in => new DateTime(in.readLong(true))),
      registerCustom[LocalTime]((dt, out) => out.writeLong(dt.getMillisOfDay, true), in => new LocalTime(in.readLong(true))),
      registerCustom[DateTimeZone]((dtz, out) => out.writeString(dtz.getID), in => DateTimeZone.forID(in.readString())),
      register[Chronology]
    )

    registerGroup("Hadoop Types", 100)(
      register[TableName],
      registerCustom[Configuration]((value, output) => value.write(new DataOutputStream(output)), input => {
        val conf = new Configuration()
        conf.readFields(new DataInputStream(input))
        conf
      })
    )

    registerGroup("Ontology Types", 300)(
      register[NodeInfo],
      register[Array[NodeInfo]],
      register[EdgeInfo],
      register[Array[Edge[EdgeInfo]]],
      register[Statement],
      register[Edge[EdgeInfo]],
      register[TraversalInfo],
      register[EnrichedOntologyNode],
      register[Array[EnrichedOntologyNode]]
    )

    registerGroup("Collection Types", 300)(
      register[ListBuffer[_]],
      register[ArrayBuffer[_]],
      register[Array[String]],
      register[Array[(_,_)]],
      register[Array[(_,_,_)]],
      register[Array[(_,_,_,_)]],
      register[Array[(_,_,_,_,_)]],
      register[Array[(_,_,_,_,_,_)]],
      register[Array[(_,_,_,_,_,_,_)]],
      registerClassName("scala.collection.mutable.WrappedArray$ofRef"),
      registerClassName("scala.collection.immutable.Set$EmptySet$")
    )
  }
}

trait GrvKryoRegistrationFunctions {

  // for registration id's in groups that have not been used, we assign this Serializer to throw an exception
  // if we try to read a value assigned to that id.  This should never happen unless a registration was inserted
  // rather than appended which could cause outdated code
  class NotAssignedSerializer(id: Int) extends Serializer[Null] {
    override def write(kryo: Kryo, output: Output, `object`: Null): Unit = {}
    override def read(kryo: Kryo, input: Input, `type`: Class[Null]): Null = {
      val current = kryo.getRegistration(id)
      val last = (1 to id).find(i => kryo.getRegistration(id - i) != null)
      throw new KryoException(s"Attempted to deserialize an id [$id] that was not registered in ${ClassName.simpleName(this)}.  Current kryo registration for id [$id] is $current, last registration that came before [$id] is $last.")
    }
  }

  def registerGroup(name: String, size: Int)(regs: Registration*)(implicit kryo: Kryo): Unit = {
    (0 until (size - regs.length)).foreach(i => kryo.register(classOf[Null], new NotAssignedSerializer(i)))
  }

  def register[T](implicit kryo: Kryo, ct: ClassTag[T], s: Serializer[T]): Registration = kryo.register(ct.runtimeClass, s)
  def register(clazz: Class[_], serializer: Serializer[_])(implicit kryo: Kryo): Registration = kryo.register(clazz, serializer)
  def registerCustom[T](out: (T, Output) => Unit, in: Input => T)(implicit kryo: Kryo, ct: ClassTag[T]): Registration = kryo.register(ct.runtimeClass, new Serializer[T] {
    override def write(kryo: Kryo, output: Output, t: T): Unit = out(t, output)
    override def read(kryo: Kryo, input: Input, aClass: Class[T]): T = in(input)
  })

  // use only when we need to register classes by name (ie: private classes)
  def registerClassName(name: String)(implicit kryo: Kryo): Registration = kryo.register(Class.forName(name))
}