package com.gravity.mapreduce

/**
 * Created by cstelzmuller on 9/11/15.
 */
abstract class ObjectToBinaryMapper extends com.gravity.hbase.mapreduce.HMapper[org.apache.hadoop.io.LongWritable, AnyRef, org.apache.hadoop.io.BytesWritable, org.apache.hadoop.io.BytesWritable] with com.gravity.hbase.mapreduce.BinaryWritable {
}
