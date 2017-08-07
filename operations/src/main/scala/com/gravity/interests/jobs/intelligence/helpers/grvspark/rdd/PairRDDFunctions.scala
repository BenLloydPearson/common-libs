package com.gravity.interests.jobs.intelligence.helpers.grvspark.rdd

import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.Partitioner._
import org.apache.spark.gravity.MergeJoin.{FullOuterJoin, InnerJoin, LeftOuterJoin, RightOuterJoin}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class PairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

}

