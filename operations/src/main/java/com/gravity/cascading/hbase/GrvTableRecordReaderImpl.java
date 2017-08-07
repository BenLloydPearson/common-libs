package com.gravity.cascading.hbase;/*             )\._.,--....,'``.      
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class GrvTableRecordReaderImpl {

  static final Log LOG = LogFactory.getLog(GrvTableRecordReaderImpl.class);

  private ResultScanner scanner = null;
  private Scan scan = null;
  private HTable htable = null;
  private byte[] lastSuccessfulRow = null;
  private ImmutableBytesWritable key = null;
  private Result value = null;
  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws java.io.IOException
   */
  public void restart(byte[] firstRow) throws IOException {
    Scan newScan = new Scan(scan);
     newScan.setStartRow(firstRow);
     this.scanner = this.htable.getScanner(newScan);
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException {
    restart(scan.getStartRow());
  }


  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.htable = htable;
  }

  /**
    * Sets the scan defining the actual details like columns etc.
    *
    * @param scan  The scan to set.
    */
   public void setScan(Scan scan) {
     this.scan = scan;
   }


  public void close() {
    this.scanner.close();
  }

  /**
   * @return ImmutableBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  /**
   * @return RowResult
   *
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public Result createValue() {
    return new Result();
  }

  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return 0;
  }

  /**
   * @param key HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   * @throws IOException
   */
  public boolean next(ImmutableBytesWritable key, Result value)
  throws IOException {
    Result result;
    try {
      result = this.scanner.next();
    } catch (DoNotRetryIOException e) {
      throw e;
    } catch (IOException e) {
      LOG.debug("recovered from " + StringUtils.stringifyException(e));
      if (lastSuccessfulRow == null) {
        LOG.warn("We are restarting the first next() invocation," +
            " if your mapper's restarted a few other times like this" +
            " then you should consider killing this job and investigate" +
            " why it's taking so long.");
      }
      if (lastSuccessfulRow == null) {
        restart(scan.getStartRow());
      } else {
        restart(lastSuccessfulRow);
        this.scanner.next();    // skip presumed already mapped row
      }
      result = this.scanner.next();
    }

    if (result != null && result.size() > 0) {
      key.set(result.getRow());
      lastSuccessfulRow = key.get();
      value.copyFrom(result);
      return true;
    }
    return false;
  }
}
