package com.gravity.cascading.hbase;

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


import com.gravity.interests.jobs.intelligence.hbase.HBaseTestTableBroker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a backport of the Hadoop MultiTableOutputFormat to the legacy mapred TableOutputFormat.
 * It keeps a dictionary of tables and does buffered writes into them.  It guarantees that when the mapper is shut down
 * it will flush writes to the tables.
 */
@SuppressWarnings({"unchecked", "deprecation"})
public class GrvTableOutputFormat extends
        FileOutputFormat<ImmutableBytesWritable, Mutation> {



  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
   * and write to an HBase table
   */
  protected static class TableRecordWriter
          implements RecordWriter<ImmutableBytesWritable, Mutation> {

    private Map<ImmutableBytesWritable,HTable> tables;

    //Why do we set this to false by default?  Because our jobs should always be reproducible, the guarantee of
    //correctness should be in the atomicity of the jobs rather than a need for their output to be correct in the face
    //of a crash.
    private boolean useWriteAheadLogging = false;

    private Configuration conf;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     */
    public TableRecordWriter(boolean useWriteAheadLogging, Configuration conf) {
      this.tables = new HashMap<ImmutableBytesWritable,HTable>();
      this.useWriteAheadLogging = useWriteAheadLogging;
      this.conf = conf;
    }

    HTable getTable(ImmutableBytesWritable tableName) throws IOException {

        if(HBaseTestTableBroker.needsTestTable()) {
            return HBaseTestTableBroker.getTable(Bytes.toString(tableName.get()));
        }else {
            if(!tables.containsKey(tableName)){
                HTable table = new HTable(conf,tableName.get());
                table.setAutoFlush(false);
                tables.put(tableName,table);
            }
            return tables.get(tableName);
        }

    }

    public void close(Reporter reporter)
            throws IOException {
      for(HTable table : tables.values()) {
        table.flushCommits();
      }
    }

    public void write(ImmutableBytesWritable key,
                      Mutation action) throws IOException {
      HTable table = getTable(key);
      if (action instanceof Put) {
         Put put = new Put((Put) action);
         put.setWriteToWAL(useWriteAheadLogging);
         table.put(put);
       } else if (action instanceof Delete) {
         Delete delete = new Delete((Delete) action);
         table.delete(delete);
       } else
         throw new IllegalArgumentException(
             "action must be either Delete or Put");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter getRecordWriter(FileSystem ignored,
                                      JobConf job, String name, Progressable progress) throws IOException {

    // expecting exactly one path

    Configuration conf = HBaseConfiguration.create(job);
    return new TableRecordWriter(false,conf);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
          throws FileAlreadyExistsException, InvalidJobConfException, IOException {
  }
}
