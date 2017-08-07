package com.gravity.interests.jobs.intelligence.helpers.grvspark.hbase

import com.gravity.hbase.schema.{ByteConverter, HbaseTable}
import com.gravity.interests.jobs.intelligence.Schema._
import com.gravity.interests.jobs.intelligence.helpers.grvspark.SerializableConfiguration
import com.gravity.interests.jobs.intelligence.{Schema, SchemaRegistry}
import com.gravity.utilities.grvfunc._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, TableName}
import org.apache.spark.Partitioner

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Region information is marked "transient lazy" which means it will be refreshed on deserialization
 *
 */
protected[hbase] class HBaseRegionPartitioner(tableName: String, val conf: SerializableConfiguration) extends Partitioner {

	@transient lazy val endKeys: Array[Array[Byte]] = {
		val conn = ConnectionFactory.createConnection(conf.value)
		try {
			conn.getRegionLocator(TableName.valueOf(tableName)).getEndKeys
		} finally {
			conn.close()
		}
	}

	@transient lazy val rowKeyConverter: ByteConverter[Any] = Schema.getTableReference(tableName).table.rowKeyConverter.asInstanceOf[ByteConverter[Any]]

	override def numPartitions: Int = endKeys.length

	override def getPartition(key: Any): Int = {
		val keyBytes = rowKeyConverter.toBytes(key)

		endKeys.indexWhere(endKey => {
			Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || Bytes.compareTo(endKey, 0, endKey.length, keyBytes, 0, keyBytes.length) > 0
		}).ifThen(_ == -1)(_ => key.hashCode() % numPartitions)

	}

}
