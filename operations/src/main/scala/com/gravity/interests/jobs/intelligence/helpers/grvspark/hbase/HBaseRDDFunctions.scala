package com.gravity.interests.jobs.intelligence.helpers.grvspark.hbase

import com.gravity.hbase.schema.{HRow, HbaseTable, OpBase, OpsResult}
import com.gravity.interests.jobs.intelligence.TableReference
import com.gravity.interests.jobs.intelligence.helpers.grvspark.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.elasticsearch.common.recycler.Recycler.V

import scala.concurrent.duration._
import scala.reflect.ClassTag

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */


/**
	* Extensions for RDD[OpBase]
	*/
class HBaseRDDOpBaseFunctions[R, O <: OpBase[_, R]](rdd: RDD[O]) {
	/**
		* Execute all ops in RDD
		*/
	def executeOps()(implicit conf: Configuration): RDD[OpsResult] = {
		val hbaseConf = rdd.context.broadcast(new SerializableConfiguration(conf))
		rdd.mapPartitions[OpsResult](_.map(_.execute()(hbaseConf.value.value)), preservesPartitioning = true)
	}
}

/**
	* Extensions for RDD[(Key, Row)]
	*/
class HBaseRDDPairFunctions[T <: HbaseTable[T, R, RR] : ClassTag, R : ClassTag, RR <: HRow[T, R] : ClassTag](rdd: RDD[(R, RR)])(implicit val tableRef: TableReference[T, R, RR]) {

	/**
		* Can be called to repartition the RDD to align with region splits for the keys.
		*/
	def repartitionByRegionServer(implicit conf: Configuration): RDD[(R, RR)] = {
		rdd.partitionBy(new HBaseRegionPartitioner(tableRef.table.tableName, new SerializableConfiguration(conf)))
	}
}

/**
	* Extensions for RDD[(Key, OpBase)]
	*/
class HBaseRDDPairOpBaseFunctions[R, O <: OpBase[_, R]](rdd: RDD[(R, O)]) {
	/**
		* Execute all ops in RDD
		*/
	def executeOps(timeout: Duration = 0 milliseconds)(implicit conf: Configuration): RDD[(R, OpsResult)] = {
		val hbaseConf = rdd.context.broadcast(new SerializableConfiguration(conf))
		rdd.mapPartitions[(R, OpsResult)](_.map(kv => kv._1 -> kv._2.execute()(hbaseConf.value.value)), preservesPartitioning = true)
	}
}

