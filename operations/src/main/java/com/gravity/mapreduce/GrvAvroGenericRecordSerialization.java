package com.gravity.mapreduce;

/**
 * Created by cstelzmuller on 10/21/15.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The {@link org.apache.hadoop.io.serializer.Serialization} used by jobs configured with {@link org.apache.avro.mapred.AvroJob}.
 */
public class GrvAvroGenericRecordSerialization extends Configured
        implements Serialization<GenericData.Record> {

    public boolean accept(Class<?> c) {
        return GenericRecord.class.isAssignableFrom(c);
    }

    /**
     * Returns the specified map output deserializer.  Defaults to the final
     * output deserializer if no map output schema was specified.
     */
    public Deserializer<GenericData.Record> getDeserializer(Class<GenericData.Record> c) {
        DatumReader<GenericData.Record> datumReader = new GenericDatumReader<GenericData.Record>();
        return new GrvAvroGenericRecordDeserializer(datumReader);
    }

    private static final DecoderFactory FACTORY = DecoderFactory.get();

    private class GrvAvroGenericRecordDeserializer
            implements Deserializer<GenericData.Record> {

        private DatumReader<GenericData.Record> reader;
        private BinaryDecoder decoder;

        public GrvAvroGenericRecordDeserializer(DatumReader<GenericData.Record> reader) {
            this.reader = reader;
        }

        public void open(InputStream in) {
            this.decoder = FACTORY.directBinaryDecoder(in, decoder);
        }

        public GenericData.Record deserialize(GenericData.Record record)
                throws IOException {
            if (record != null) reader.setSchema(record.getSchema());
            GenericData.Record datum = reader.read(record == null ? null : record, decoder);

            return datum;
        }

        public void close() throws IOException {
            decoder.inputStream().close();
        }

    }


    /**
     * Returns the specified output serializer.
     */
    public Serializer<GenericData.Record> getSerializer(Class<GenericData.Record> c) {
        return new GrvAvroGenericRecordSerializer(new GenericDatumWriter<GenericData.Record>());
    }

    /**
     * AvroSpecificRecordSerialization was added primarily for Scalding interop since Kryo
     * has trouble with some nested Avro records. The only time you need to use it in
     * Cascading is if you have Avro records inside a tuple and then do an operation that
     * forces a reduce. In this case Hadoop doesn't know how to serialize Avro records
     * so this class plugs in to do that.
     *
     * Add it to the list of other serialization classes in "io.serializations" property.
     * The order is important, especially if Kryo is configured to accept all classes.
     * In this case you want this serializer to come first in the list.
     *
     * FWIW - this has nothing to do with Scalding. It's usable and useful for all DSLs and
     * straight up Cascading
     *
     */
    private class GrvAvroGenericRecordSerializer implements Serializer<GenericData.Record> {

        private DatumWriter<GenericData.Record> writer;
        private OutputStream out;
        private BinaryEncoder encoder;

        public GrvAvroGenericRecordSerializer(DatumWriter<GenericData.Record> writer) {
            this.writer = writer;
        }

        public void open(OutputStream out) {
            this.out = out;
            this.encoder = new EncoderFactory().configureBlockSize(512)
                    .binaryEncoder(out, null);
        }

        public void serialize(GenericData.Record record) throws IOException {
            writer.setSchema(record.getSchema());
            writer.write(record, encoder);
            // would be a lot faster if the Serializer interface had a flush()
            // method and the Hadoop framework called it when needed rather
            // than for every record.
            encoder.flush();
        }

        public void close() throws IOException {
            out.close();
        }

    }

}