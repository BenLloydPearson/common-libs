package com.gravity.interests.jobs.intelligence.helpers

import com.gravity.hbase.schema.{HRow, HbaseTable, OpBase}
import com.gravity.interests.jobs.intelligence.TableReference
import com.gravity.interests.jobs.intelligence.helpers.grvspark.hbase.{HBaseRDDOpBaseFunctions, HBaseRDDPairOpBaseFunctions, HBaseRDDPairFunctions}
import com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.MLLibFunctions
import com.gravity.interests.jobs.intelligence.helpers.grvspark.rdd.{PairRDDFunctions, RDDFunctions}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
package object grvspark extends MLLibFunctions {

	// SparkContext functions
	implicit def sparkFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)

	// RDD[V] functions
	implicit def rddFunctions[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions[T](rdd)

	// RDD[(K, V)] functions
	implicit def pairFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): PairRDDFunctions[K, V] = new PairRDDFunctions[K, V](rdd)

	// HBase RDD[(R, RR]) functions
	implicit def toHBaseRDDPairFunctions[T <: HbaseTable[T, R, RR] : ClassTag, R : ClassTag, RR <: HRow[T, R] : ClassTag](rdd: RDD[(R, RR)])(implicit tableRef: TableReference[T, R, RR]): HBaseRDDPairFunctions[T, R, RR] = new HBaseRDDPairFunctions[T, R, RR](rdd)

	// HBase RDD[(R, OpBase)] functions
	implicit def toHBaseRDDOpBaseFunctions[R, O <: OpBase[_, R]](rdd: RDD[O]): HBaseRDDOpBaseFunctions[R, O] = new HBaseRDDOpBaseFunctions[R, O](rdd)

	// HBase RDD[OpBase] functions
	implicit def toHBaseRDDPairOpBaseFunctions[R, O <: OpBase[_, R]](rdd: RDD[(R, O)]): HBaseRDDPairOpBaseFunctions[R, O] = new HBaseRDDPairOpBaseFunctions[R, O](rdd)

}
