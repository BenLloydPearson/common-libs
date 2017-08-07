package com.gravity.utilities;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;

/**
 * Created by erik on 6/3/15.
 */
@SuppressWarnings("deprecation")
public class DeprecatedHBase {
    private DeprecatedHBase() {}

    public static boolean delete(FileSystem fs, Path p) throws IOException {
        return fs.delete(p);
    }

    public static void setDeferredLogFlushOn(HTableDescriptor hTableDescriptor) {
        hTableDescriptor.setDeferredLogFlush(true);
    }

    public static void setEncodeOnDiskOff(HColumnDescriptor hColumnDescriptor) {
        hColumnDescriptor.setEncodeOnDisk(false);
    }

    public static void setWriteToWAL(org.apache.hadoop.hbase.client.Mutation mutation, Boolean write) {
        mutation.setWriteToWAL(write);
    }

    public static String hbaseRegionServerLeasePeriodKey() {
        return HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY;
    }
}
