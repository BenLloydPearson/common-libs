/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package com.gravity.cascading.hbase;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.gravity.hbase.mapreduce.Settings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * Gravity NOTE: This is adapted from the Twitter Maple project
 *
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with the {@HBaseFullScheme}
 * to allow for the reading and writing of data to and from a HBase cluster.
 */
public class GrvHBaseRowTap extends Tap<Configuration, RecordReader, OutputCollector> {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger(GrvHBaseRowTap.class);
  private final String id = UUID.randomUUID().toString();

  private String scanStr;

  /** Field hostName */
  private String quorumNames;
  /** Field tableName */
  private String tableName;

  /**
   * Constructor HBaseTap creates a new HBaseTap instance.
   *
   * @param tableName       of type String
   * @param HBaseFullScheme of type HBaseFullScheme
   */
  public GrvHBaseRowTap(String tableName, GrvHBaseRowScheme HBaseFullScheme, Scan scan) {
    super(HBaseFullScheme, SinkMode.UPDATE);
    this.tableName = tableName;
    this.scanStr = convertScanToString(scan);
  }

    public GrvHBaseRowTap(GrvHBaseRowScheme HBaseFullScheme) {
        super(HBaseFullScheme, SinkMode.UPDATE);
    }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
//    conf.set("hbase.zookeeper.quorum", "grv-hadoopc14.lax1.gravity.com,grv-hadoopc09.lax1.gravity.com,grv-hadoopc04.lax1.gravity.com,grv-hadoopm02.lax1.gravity.com,grv-hadoopc19.lax1.gravity.com");
    if (isReplace() && conf.get("mapred.task.partition") == null) {
      try {
        deleteResource(conf);
      } catch (IOException e) {
        throw new RuntimeException("could not delete resource: " + e);
      }
    }

    super.sinkConfInit(process, conf);
  }

  @Override public String getIdentifier() {
    return id;
  }

  @Override public TupleEntryIterator openForRead(FlowProcess<? extends Configuration> jobConfFlowProcess,
      RecordReader recordReader) throws IOException {
      return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
  }

  @Override public TupleEntryCollector openForWrite(FlowProcess<? extends Configuration> jobConfFlowProcess,
      OutputCollector outputCollector) throws IOException {
      GrvHBaseTapCollector hBaseCollector = new GrvHBaseTapCollector( jobConfFlowProcess, this );
         hBaseCollector.prepare();
         return hBaseCollector;
  }

  @Override public boolean createResource(Configuration jobConf) throws IOException {
    return true;
  }

  @Override public boolean deleteResource(Configuration jobConf) throws IOException {

    return true;
  }

  @Override public boolean resourceExists(Configuration jobConf) throws IOException {
    return true;
  }

  @Override public long getModifiedTime(Configuration jobConf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> process, Configuration conf) {
    LOG.debug("sourcing from table: {}", tableName);

    FileInputFormat.addInputPaths((JobConf)conf, tableName);
    conf.set(GrvTableInputFormat.INPUT_TABLE, tableName);
    conf.set(GrvTableInputFormat.SCAN,scanStr);


    super.sourceConfInit(process, conf);
  }

  private String convertScanToString(Scan scan) {
    try {
      return Settings.convertScanToString(scan);
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }


  @Override
  public boolean equals(Object object) {
    if (this == object) { return true; }
    if (object == null || getClass() != object.getClass()) { return false; }
    if (!super.equals(object)) { return false; }

    GrvHBaseRowTap hBaseTap = (GrvHBaseRowTap) object;

    if (tableName != null ? !tableName.equals(hBaseTap.tableName) : hBaseTap.tableName != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    return result;
  }
}
