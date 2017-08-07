package com.gravity.test

import java.util.Date

import com.gravity.interests.jobs.intelligence.helpers.grvspark.GrvSparkKryoRegistrator
import com.gravity.utilities.BaseScalaTest
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
trait sparkTesting {
 import com.gravity.logging.Logging._

	def withSparkContext[T](work: SparkContext => T): T = {
		val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString

	  val conf = new SparkConf().
	    setMaster("local[*]").
	    setAppName("test").
	    set("spark.ui.enabled", "false").
	    set("spark.app.id", appID).
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
		  set("spark.kryo.registrator", classOf[GrvSparkKryoRegistrator].getName)

		val sc = new SparkContext(conf)
		try {
			work(sc)
		} finally {
			sc.stop()
		}
	}
}
