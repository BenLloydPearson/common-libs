package com.gravity.cascading.hbase;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is a scheme for Cascading that uses a TableInputFormat that was backported to the old mapreduce API, thus allowing full scanner/filtration functionality.
 *
 * It emits two tuple values: 'k' is the bytes in the row key, and 'v' is the actual Result object.
 *
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

 public class GrvHBaseRowScheme
    extends Scheme<Configuration, RecordReader, OutputCollector, Object[], Object[]> {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(GrvHBaseRowScheme.class);

  /** Field keyFields */
  private Fields tableField = new Fields("t");
  private Fields putField = new Fields("p");
  private Fields keyField = new Fields("k");
  private Fields valueField = new Fields("v");


  public GrvHBaseRowScheme() {
    setSourceSink();
  }

  private void validate() {
    if (keyField.size() != 1) {
      throw new IllegalArgumentException("may only have one key field, found: " + keyField.print());
    }
  }

  private void setSourceSink() {

    Fields sourceFields = new Fields("k","r");
    Fields sinkFields = new Fields("t","p");

    setSourceFields(Fields.join(keyField,valueField));
    setSinkFields(Fields.join(tableField,putField));
  }


  @Override
  public void sourcePrepare(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    Object[] pair =
        new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

    sourceCall.setContext(pair);
  }

  @Override
  public void sourceCleanup(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) {
    sourceCall.setContext(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean source(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    Tuple result = new Tuple();

    Object key = sourceCall.getContext()[0];
    Object value = sourceCall.getContext()[1];
    boolean hasNext = sourceCall.getInput().next(key, value);
    if (!hasNext) { return false; }

    ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
    Result row = (Result) value;

    result.add(keyWritable.get());
    result.add(row);

    sourceCall.getIncomingEntry().setTuple(result);

    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void sink(FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall)
      throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
    Tuple putTuple = tupleEntry.selectTuple(putField);
    Tuple tableTuple = tupleEntry.selectTuple(tableField);
    ImmutableBytesWritable tableName = (ImmutableBytesWritable)tableTuple.getObject(0);
    Mutation put = (Mutation)putTuple.getObject(0);

    OutputCollector outputCollector = sinkCall.getOutput();
    outputCollector.collect(tableName,put);

  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> process,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
    JobConf jobConf = (JobConf)conf;
    jobConf.setOutputFormat(GrvTableOutputFormat.class);

    jobConf.setOutputKeyClass(ImmutableBytesWritable.class);
    jobConf.setOutputValueClass(Writable.class);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> process,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf) {
      JobConf jobConf = (JobConf)conf;
    jobConf.setInputFormat(GrvTableInputFormat.class);

  }


  @Override
  public boolean equals(Object object) {
    if (this == object) { return true; }
    if (object == null || getClass() != object.getClass()) { return false; }
    if (!super.equals(object)) { return false; }

    GrvHBaseRowScheme that = (GrvHBaseRowScheme) object;

    if (keyField != null ? !keyField.equals(that.keyField) : that.keyField != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (keyField != null ? keyField.hashCode() : 0);
    return result;
  }
}
