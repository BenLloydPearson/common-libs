package com.gravity.mapreduce;

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import com.gravity.hadoop.GravityTableOutputFormat;
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestTableBroker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

@SuppressWarnings({"unchecked", "deprecation"})
public class GravityTableOutputFormatForTesting<KEY> extends OutputFormat<KEY, Mutation>
        implements Configurable {

    private final Log LOG = LogFactory.getLog(GravityTableOutputFormat.class);

    /** Job parameter that specifies the output table. */
    public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";

    /**
     * Optional job parameter to specify a peer cluster.
     * Used specifying remote cluster when copying between hbase clusters (the
     * source is picked up from <code>hbase-site.xml</code>).
     */
    public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";

    /** Optional specification of the rs class name of the peer cluster */
    public static final String
            REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
    /** Optional specification of the rs impl name of the peer cluster */
    public static final String
            REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";


    /** The configuration. */
    private Configuration conf = null;

    private HTable table;

    /**
     * Writes the reducer output to an HBase table.
     *
     * @param <KEY>  The type of the key.
     */
    protected static class TableRecordWriter<KEY>
            extends RecordWriter<KEY, Mutation> {

        /** The table to write to. */
        private HTable table;

        /**
         * Instantiate a TableRecordWriter with the HBase HClient for writing.
         *
         * @param table  The table to write to.
         */
        public TableRecordWriter(HTable table) {
            this.table = table;
        }

        /**
         * Closes the writer, in this case flush table commits.
         *
         * @param context  The context.
         * @throws java.io.IOException When closing the writer fails.
         * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
         */
        @Override
        public void close(TaskAttemptContext context)
                throws IOException {
            table.flushCommits();
            // The following call will shutdown all connections to the cluster from
            // this JVM.  It will close out our zk session otherwise zk wil log
            // expired sessions rather than closed ones.  If any other HTable instance
            // running in this JVM, this next call will cause it damage.  Presumption
            // is that the above this.table is only instance.

//      HConnectionManager.deleteAllConnections(true);

        }

        /**
         * Writes a key/value pair into the table.
         *
         * @param key  The key.
         * @param value  The value.
         * @throws IOException When writing fails.
         * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
         */
        @Override
        public void write(KEY key, Mutation value)
                throws IOException {
            if (value instanceof Put) {
                ((Put)value).setWriteToWAL(false);
                this.table.put(new Put((Put)value));

            }
            else if (value instanceof Delete) this.table.delete(new Delete((Delete)value));
            else throw new IOException("Pass a Delete or a Put");
        }
    }

    /**
     * Creates a new record writer.
     *
     * @param context  The current task context.
     * @return The newly created writer instance.
     * @throws IOException When creating the writer fails.
     * @throws InterruptedException When the jobs is cancelled.
     * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordWriter<KEY, Mutation> getRecordWriter(
            TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TableRecordWriter<KEY>(this.table);
    }

    /**
     * Checks if the output target exists.
     * NOTE: Currently not implemented.
     *
     * @param context  The current context.
     * @throws IOException When the check fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
     */
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        // Currently not needed. Implement if you ever need it.

    }

    /**
     * Returns the output committer.
     *
     * @param context  The current context.
     * @return The committer.
     * @throws IOException When creating the committer fails.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TableOutputCommitter();
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration otherConf) {
        this.conf = otherConf;
        String tableName = this.conf.get(OUTPUT_TABLE);

        this.table = HBaseTestTableBroker.getTable(tableName);
    }
}
