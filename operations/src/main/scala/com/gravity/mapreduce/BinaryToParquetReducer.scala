package com.gravity.mapreduce

import org.apache.avro.generic.GenericData.Record


/**
 * Created by cstelzmuller on 5/18/15.
 */
abstract class BinaryToParquetReducer extends com.gravity.hbase.mapreduce.HReducer[org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.NullWritable, Record] with com.gravity.hbase.mapreduce.BinaryReadable {

}
