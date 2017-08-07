package com.gravity.cascading.hbase;

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

import com.gravity.hbase.mapreduce.Settings;
import com.gravity.interests.jobs.intelligence.hbase.HBaseTestTableBroker;
import com.gravity.interests.jobs.intelligence.hbase.UnitTestTable;
import com.gravity.utilities.ScalaMagic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapred.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The backports the functionality of the non deprecated TableInputFormat to the deprecated TableInputFormat interface.  This provides a lot of new functionality to Cascading.
 */
@SuppressWarnings("deprecation")
public class GrvTableInputFormat implements
        JobConfigurable, InputFormat<ImmutableBytesWritable, Result> {
    private final Log LOG = LogFactory.getLog(GrvTableInputFormat.class);

    private JobConf conf = null;
    private Scan scan = null;


    private HTable table;
    /**
     * space delimited list of columns
     */
    public static final String COLUMN_LIST = "hbase.mapred.tablecolumns";
    /**
     * Job parameter that specifies the input table.
     */
    public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
    /**
     * Base-64 encoded scanner. All other SCAN_ confs are ignored if this is specified.
     * See {@link org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil#convertScanToString(org.apache.hadoop.hbase.client.Scan)} for more details.
     */
    public static final String SCAN = "hbase.mapreduce.scan";
    /**
     * Column Family to Scan
     */
    public static final String SCAN_COLUMN_FAMILY = "hbase.mapreduce.scan.column.family";
    /**
     * Space delimited list of columns to scan.
     */
    public static final String SCAN_COLUMNS = "hbase.mapreduce.scan.columns";
    /**
     * The timestamp used to filter columns with a specific timestamp.
     */
    public static final String SCAN_TIMESTAMP = "hbase.mapreduce.scan.timestamp";
    /**
     * The starting timestamp used to filter columns with a specific range of versions.
     */
    public static final String SCAN_TIMERANGE_START = "hbase.mapreduce.scan.timerange.start";
    /**
     * The ending timestamp used to filter columns with a specific range of versions.
     */
    public static final String SCAN_TIMERANGE_END = "hbase.mapreduce.scan.timerange.end";
    /**
     * The maximum number of version to return.
     */
    public static final String SCAN_MAXVERSIONS = "hbase.mapreduce.scan.maxversions";
    /**
     * Set to false to disable server-side caching of blocks for this scan.
     */
    public static final String SCAN_CACHEBLOCKS = "hbase.mapreduce.scan.cacheblocks";
    /**
     * The number of rows for caching that will be passed to scanners.
     */
    public static final String SCAN_CACHEDROWS = "hbase.mapreduce.scan.cachedrows";


    /**
     * Builds a TableRecordReader. If no TableRecordReader was provided, uses
     * the default.
     *
     * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
     *      JobConf, org.apache.hadoop.mapred.Reporter)
     */
    public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        org.apache.hadoop.hbase.mapred.TableSplit tSplit = (org.apache.hadoop.hbase.mapred.TableSplit) split;
        GrvTableRecordReader trr =  new GrvTableRecordReader();


        Scan sc = new Scan(this.scan);
        sc.setStartRow(tSplit.getStartRow());
        sc.setStopRow(tSplit.getEndRow());
        trr.setScan(sc);
        trr.setHTable(table);
        trr.init();


        return trr;
    }

    public void configure(JobConf job) {

        setConf(job);
    }

    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param base64 The scan details.
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    static Scan convertStringToScan(String base64) throws IOException {
        return Settings.convertStringToScan(base64);
    }


    public void setConf(JobConf configuration) {
        this.conf = configuration;
        String tableName = conf.get(INPUT_TABLE);
        try {
            if(HBaseTestTableBroker.needsTestTable()) {
                setHTable(HBaseTestTableBroker.getTable(tableName));
            }else {
                setHTable(new HTable(HBaseConfiguration.create(configuration), tableName));
            }
        } catch (Exception e) {
            ScalaMagic.printException("Exception in job",e);
        }

        Scan scan = null;

        if (conf.get(SCAN) != null) {
            try {
                scan = convertStringToScan(conf.get(SCAN));
            } catch (IOException e) {
                LOG.error("An error occurred.", e);
            }
        } else {
            try {
                scan = new Scan();

                if (conf.get(SCAN_COLUMNS) != null) {

                    throw new RuntimeException("This was deprecated in hbase .92, and we should be using the serialized scanner");
                    //scan.addColumns(conf.get(SCAN_COLUMNS));
                }

                if (conf.get(SCAN_COLUMN_FAMILY) != null) {
                    scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
                }

                if (conf.get(SCAN_TIMESTAMP) != null) {
                    scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
                }

                if (conf.get(SCAN_TIMERANGE_START) != null && conf.get(SCAN_TIMERANGE_END) != null) {
                    scan.setTimeRange(
                            Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
                            Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
                }

                if (conf.get(SCAN_MAXVERSIONS) != null) {
                    scan.setMaxVersions(Integer.parseInt(conf.get(SCAN_MAXVERSIONS)));
                }

                if (conf.get(SCAN_CACHEDROWS) != null) {
                    scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
                }

                // false by default, full table scans generate too much BC churn
                scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
            } catch (Exception e) {
                ScalaMagic.printException("Exception in job",e);
            }
        }

        setScan(scan);
    }

    public void setScan(Scan scanToSet) {
        scan = scanToSet;
    }


    /**
     * Calculates the splits that will serve as input for the map tasks.
     * <ul>
     * Splits are created in number equal to the smallest between numSplits and
     * the number of {@link org.apache.hadoop.hbase.regionserver.HRegion}s in the table. If the number of splits is
     * smaller than the number of {@link org.apache.hadoop.hbase.regionserver.HRegion}s then splits are spanned across
     * multiple {@link org.apache.hadoop.hbase.regionserver.HRegion}s and are grouped the most evenly possible. In the
     * case splits are uneven the bigger splits are placed first in the
     * {@link InputSplit} array.
     *
     * @param job       the map task {@link JobConf}
     * @param numSplits a hint to calculate the number of splits (mapred.map.tasks).
     * @return the input splits
     * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
     */
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        if (table == null) {
            throw new IOException("No table was provided.");
        }
        if( table instanceof UnitTestTable) {
            InputSplit[] splits = new InputSplit[1];
            splits[0] = new org.apache.hadoop.hbase.mapred.TableSplit(table.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, "local");
            return splits;
        }else {
            Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
            if (keys == null || keys.getFirst() == null ||
                    keys.getFirst().length == 0) {
                throw new IOException("Expecting at least one region.");
            }
            int count = 0;
            List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
            for (int i = 0; i < keys.getFirst().length; i++) {
                if (!includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
                    continue;
                }
                String regionLocation = table.getRegionLocation(keys.getFirst()[i]).getHostname();
                byte[] startRow = scan.getStartRow();
                byte[] stopRow = scan.getStopRow();
                // determine if the given start an stop key fall into the region
                if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
                        Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
                        (stopRow.length == 0 ||
                                Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
                    byte[] splitStart = startRow.length == 0 ||
                            Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
                            keys.getFirst()[i] : startRow;
                    byte[] splitStop = (stopRow.length == 0 ||
                            Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
                            keys.getSecond()[i].length > 0 ?
                            keys.getSecond()[i] : stopRow;
                    InputSplit split = new org.apache.hadoop.hbase.mapred.TableSplit(table.getTableName(),
                            splitStart, splitStop, regionLocation);
                    splits.add(split);
                    if (LOG.isDebugEnabled()) { LOG.debug("getSplits: split -> " + (count++) + " -> " + split); }
                }
            }
            InputSplit[] resultSplits = new InputSplit[splits.size()];

            for (int i = 0; i < splits.size(); i++) {
                resultSplits[i] = splits.get(i);
            }
            return resultSplits;
        }
    }


    /**
     * Test if the given region is to be included in the InputSplit while splitting
     * the regions of a table.
     * <p/>
     * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
     * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
     * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R processing,
     * continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due to the ordering of the keys.
     * <br>
     * <br>
     * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region.
     * <br>
     * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded( i.e. all regions are included).
     *
     * @param startKey Start key of the region
     * @param endKey   End key of the region
     * @return true, if this region needs to be included as part of the input (default).
     */
    protected boolean includeRegionInSplit(final byte[] startKey, final byte[] endKey) {
        return true;
    }



    /**
     * Allows subclasses to set the {@link HTable}.
     *
     * @param table to get the data from
     */
    protected void setHTable(HTable table) {
        this.table = table;
    }


}