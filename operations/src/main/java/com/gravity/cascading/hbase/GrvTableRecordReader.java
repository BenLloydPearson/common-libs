package com.gravity.cascading.hbase;/*             )\._.,--....,'``.      
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GrvTableRecordReader implements RecordReader<ImmutableBytesWritable, Result> {
  private static final Logger logger = LoggerFactory.getLogger(GrvTableRecordReader.class);

  private GrvTableRecordReaderImpl recordReaderImpl = new GrvTableRecordReaderImpl();

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * @param firstRow
     * @throws java.io.IOException
     */
    public void restart(byte[] firstRow) throws IOException {
      this.recordReaderImpl.restart(firstRow);
    }

    /**
     * Build the scanner. Not done in constructor to allow for extension.
     *
     * @throws IOException
     */
    public void init() throws IOException {
      this.recordReaderImpl.init();
    }

    /**
     * @param htable the {@link org.apache.hadoop.hbase.client.HTable} to scan.
     */
    public void setHTable(HTable htable) {
      this.recordReaderImpl.setHTable(htable);
    }

  /**
    * Sets the scan defining the actual details like columns etc.
    *
    * @param scan  The scan to set.
    */
   public void setScan(Scan scan) {
     this.recordReaderImpl.setScan(scan);
   }


    public void close() {
      this.recordReaderImpl.close();
    }

    /**
     * @return ImmutableBytesWritable
     *
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    public ImmutableBytesWritable createKey() {
      return this.recordReaderImpl.createKey();
    }

    /**
     * @return RowResult
     *
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    public Result createValue() {
      return this.recordReaderImpl.createValue();
    }

    public long getPos() {

      // This should be the ordinal tuple in the range;
      // not clear how to calculate...
      return this.recordReaderImpl.getPos();
    }

    public float getProgress() {
      // Depends on the total number of tuples and getPos
      return this.recordReaderImpl.getPos();
    }

    /**
     * @param key HStoreKey as input key.
     * @param value MapWritable as input value
     * @return true if there was more data
     * @throws IOException
     */
    public boolean next(ImmutableBytesWritable key, Result value)
    throws IOException {
      return this.recordReaderImpl.next(key, value);
    }
}
