package com.gravity.mapreduce

import com.twitter.chill.{KryoBase, ScalaKryoInstantiator}
import org.apache.avro.generic.GenericData

/**
 * Created by cstelzmuller on 11/10/15.
 */
class GrvChillKryoInstantiator extends ScalaKryoInstantiator {
  override def newKryo: KryoBase = {
    val newK = super.newKryo()
    newK.register(classOf[GenericData.Record], GrvAvroConvertibleGenericRecordSerializer)
    newK
  }
}
