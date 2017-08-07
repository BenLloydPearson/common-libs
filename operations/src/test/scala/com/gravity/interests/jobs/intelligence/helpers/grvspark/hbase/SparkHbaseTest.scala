package com.gravity.interests.jobs.intelligence.helpers.grvspark.hbase

import com.gravity.interests.jobs.intelligence.{CampaignRow, CampaignStatus, Schema, SiteRow}
import com.gravity.test.operationsTesting
import Schema._
import com.gravity.utilities
import com.gravity.utilities.BaseScalaTest
import org.apache.spark.storage.StorageLevel

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class SparkHbaseTest extends BaseScalaTest with operationsTesting {

	import com.gravity.interests.jobs.intelligence.helpers.grvspark._

	test("simple query") {
		withCleanup {
			withSparkContext { sc => {
				withSites(5) { sitesContext => {
					val rdd = sc.hbaseQuery(Schema.Sites)(_.withColumns(_.name, _.guid))
					val sites = rdd.collect()
					assertResult(5)(sites.length)
				}}
			}}
		}
	}

	test("filter query") {
		withCleanup {
			withSparkContext { sc => {
				withSites(5) { sitesContext => {
					val rdd = sc.hbaseQuery(Schema.Sites)(_.withColumns(_.name, _.guid).filter(_.and(_.columnValueMustEqual(_.guid, sitesContext.siteRows.head.guid))))
					val sites = rdd.collect()
					assertResult(1)(sites.length)
				}}
			}}
		}
	}

	/**
		* Test that persist doesn't throw exceptiosn
		*/
	test("persist rdd") {
		withCleanup {
			withSparkContext { sc => {
				withSites(5) { sitesContext => {
					val rdd = sc.hbaseQuery(Schema.Sites)(_.withColumns(_.name, _.guid))
					val persisted = rdd.persist(StorageLevel.MEMORY_AND_DISK)
					val sites = persisted.collect()
					assertResult(5)(sites.length)
					val sites2 = persisted.collect()
					assertResult(5)(sites2.length)
					persisted.unpersist(true)
				}}
			}}
		}
	}

	test("join sites and campaigns") {
		withCleanup {
			withSparkContext { sc => {
				withSites(5) { sitesContext => {
					withCampaigns(2, sitesContext) { campaigns => {
						val sites = sc.hbaseQuery(Schema.Sites)(_.withColumns(_.name, _.guid))
						val campaigns = sc.hbaseQuery(Schema.Campaigns)(_.withColumns(_.siteGuid, _.name))

						val siteCampaigns = sites.join(campaigns.map { case (k, v) => k.siteKey -> v}).aggregateByKey(Seq.empty[CampaignRow])((acc, cur) => acc :+ cur._2, _ ++ _).collect()
						assertResult(5)(siteCampaigns.length)
						siteCampaigns.foreach(sc => {
							assertResult(2)(sc._2.size)
						})
					}}
				}}
			}}
		}
	}

	test("execute puts") {
		withCleanup {
			withSparkContext { sc => {
				withSites(5) { sitesContext => {
					withCampaigns(2, sitesContext) { campaigns => {
						// select camaigns
						val campaigns = sc.hbaseQuery(Schema.Campaigns)(_.withColumns(_.siteGuid, _.name, _.status))
						campaigns.cache()

						// create an update for each campaign
						val ops = campaigns.mapPartitions(_.map{ case(k, v) => k -> Schema.Campaigns.put(k).value(_.status, CampaignStatus.completed) })
						val updated = ops.executeOps()

						val results = updated.join(campaigns).collect()
						results.foreach(kv => {
							assertResult(1)(kv._2._1.numPuts)
						})
					}}
				}}
			}}
		}
	}

}
