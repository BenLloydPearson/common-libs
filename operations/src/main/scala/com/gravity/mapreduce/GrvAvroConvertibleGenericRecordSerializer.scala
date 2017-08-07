package com.gravity.mapreduce

import com.gravity.domain.FieldConverters._
import com.twitter.chill._
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8

/**
 * Created by cstelzmuller on 11/11/15.
 */

object GrvAvroConvertibleGenericRecordSerializer extends com.esotericsoftware.kryo.Serializer[GenericData.Record] {
  override def write(kryo: Kryo, output: Output, `object`: Record): Unit = {
    val categoryName = `object`.get("categoryName").toString
    if (categoryName.isEmpty) throw new IllegalArgumentException("Not a convertible Generic Record. See Field Converters.")
    output.writeString(categoryName)

    val writer = new GenericDatumWriter[GenericData.Record](`object`.getSchema)
    val encoder = EncoderFactory.get().directBinaryEncoder(output, null)
    writer.write(`object`, encoder)
 }

  override def read(kryo: Kryo, input: Input, `type`: Class[Record]): Record = {
    val categoryName = input.readString()
    val schema = converterByName(categoryName).avroSchema
    val reader = new GenericDatumReader[GenericData.Record](schema)
    val decoder = DecoderFactory.get().directBinaryDecoder(input, null)
    reader.read(null, decoder)
  }
}
