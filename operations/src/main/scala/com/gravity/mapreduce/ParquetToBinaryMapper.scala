package com.gravity.mapreduce

import org.apache.avro.generic.GenericData.Record

/**
 * Created by cstelzmuller on 5/29/15.
 */
abstract class ParquetToBinaryMapper() extends com.gravity.hbase.mapreduce.HMapper[org.apache.hadoop.io.LongWritable, Record, org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.BytesWritable] with com.gravity.hbase.mapreduce.BinaryWritable {
}
