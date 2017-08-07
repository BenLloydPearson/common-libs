package com.gravity.interests.jobs.intelligence.spark

import java.util.logging.Level

import com.gravity.interests.jobs.intelligence.hbase.HBaseTestEnvironment
import com.gravity.test.operationsTesting
import com.gravity.utilities.{BaseScalaTest, ClassName}
import org.apache.commons.io.output.NullOutputStream
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.hindog.spark.rdd._

import scala.collection._
import scala.reflect.ClassTag
import scala.util.Random

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SparkJoinSandboxTest extends BaseScalaTest with HBaseTestEnvironment with operationsTesting {

	implicit class RDDFunctions[T : ClassTag](rdd: RDD[T]) {

		def dump(name: String, maxRows: Int = 20) = {
			val res = rdd.take(maxRows)
			val sb = new StringBuffer()

			sb.append(s"$name: RDD[${ClassName.simpleName(implicitly[ClassTag[T]].runtimeClass)}]\n")
			sb.append(s"-------- Lineage -----------\n")
			sb.append(rdd.toDebugString + "\n")
			sb.append(s"-------- Partitions [${rdd.getNumPartitions}]---------\n")
			sb.append(rdd.partitions.foreach(p => sb.append(p + "\n")) + "\n")
			sb.append("--------- Data --------------\n")
			res.foreach(v => sb.append(v + "\n"))
			sb.append("... (omitted)")
			println(sb.toString.replace("\n", s"\n$name: "))
			rdd
		}
	}


	def testJoin[V : ClassTag](name: String, mergeJoin: (RDD[(Int, String)], RDD[(Int, String)]) => RDD[(Int, V)], join: (RDD[(Int, String)], RDD[(Int, String)]) => RDD[(Int, V)])(implicit sc: SparkContext): Unit = {
		import com.gravity.grvlogging._
		// temporarily cut down on the amount of spark logging
		withLogLevel("org.apache.spark", Level.WARNING) {
			val numKeys = 26
			val valuesPerKey = 2
			val data = (0 until valuesPerKey).flatMap(_ => (0 until numKeys).map(i => i -> Random.alphanumeric.take(5).mkString))

			val rand = new Random()

			// test some random joins with randomized ordering/sizes on both sides
			(0 until 10).foreach { i =>
				val left = rand.shuffle(data).take(rand.nextInt(data.size))
				val right = rand.shuffle(data).take(rand.nextInt(data.size))

				// also randomize the input partition count
				val rdd1 = sc.parallelize(left, rand.nextInt(4) + 1)
				val rdd2 = sc.parallelize(right, rand.nextInt(4) + 1)

				var mergeJoinResult: RDD[(Int, V)] = null
				var joinResult: RDD[(Int, V)] = null
				var expected: Set[(Int, V)] = null
				var actual: Set[(Int, V)] = null
			  try {
				  Console.withOut(new NullOutputStream) {
					  mergeJoinResult = mergeJoin(rdd1, rdd2)
					  joinResult = join(rdd1, rdd2)
					  expected = joinResult.collect().toSet
					  actual = mergeJoinResult.collect().toSet
				  }
				  assertResult(true)(expected != null)
				  assertResult(true)(actual != null)
				  assertResult(expected)(actual)
			  } catch {
				  case ex: Throwable => {
					  // dump the failed data for analysis
					  rdd1.dump("rdd1", 100000)
					  rdd2.dump("rdd2", 100000)
					  mergeJoinResult.dump(s"$name [actual]", 100000)
					  joinResult.dump(s"$name [expected]", 100000)
					  throw ex
				  }
			  }
			}
		}
	}

	test("test mergeJoin") {
		withSparkContext { implicit sc =>
			testJoin("innerJoin", (l, r) => l.mergeJoin(r), (l, r) => l.join(r))
		}
	}

	test("test leftOuterMergeJoin") {
		withSparkContext { implicit sc =>
			testJoin("leftOuterJoin", (l, r) => l.leftOuterMergeJoin(r), (l, r) => l.leftOuterJoin(r))
		}
	}

	test("test rightOuterMergeJoin") {
		withSparkContext { implicit sc =>
			testJoin("rightOuterJoin", (l, r) => l.rightOuterMergeJoin(r), (l, r) => l.rightOuterJoin(r))
		}
	}

	test("test fullOuterMergeJoin") {
		withSparkContext { implicit sc =>
			testJoin("fullOuterJoin", (l, r) => l.fullOuterMergeJoin(r), (l, r) => l.fullOuterJoin(r))
		}
	}
}