package com.gravity.utilities.cache.user

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.utilities.cache.PermaCacher

/**
  * Created by akash on 8/22/16.
  */

case class NormaliziationParams(p: Double, theta: Double)

case class CalibrationParams(numeric: Double,positivity: Double,negativity: Double)

object UserFeedbackContentQualityCache {
 import com.gravity.logging.Logging._
	import com.gravity.utilities.Counters._

	val normalizationParamHdfsPath =   "/user/gravity/reports/userfeedback_normalizationparameters.csv"
	val calibrationParamHdfsPath = "/user/gravity/reports/userfeedback_calibrationparameters.csv"

	def validKeys = getValidKeys()

  val reloadFrequencySeconds = 60 * 60 // hourly

  def getNormalizationParams(variant: Int, opt: Int) : Option[NormaliziationParams] = {
		val key = getKey(variant, opt)
		val res = normailizationParamsMap.get(key)
		if (res.isEmpty) {
			countPerSecond("cqi cache", "miss on normalization params for variant, opt: " + variant + ", " + opt)
			error("cqi cache miss on normalization params for variant, opt: " + variant + ", " + opt)
		}
		res
	}

	def getCalibrationParams(variant: Int, opt: Int) : Option[CalibrationParams] = {
		val key = getKey(variant, opt)
		val res = calibrationParamsMap.get(key)
		if (res.isEmpty) {
			countPerSecond("cqi cache", "miss on calibration params for variant, opt: " + variant + ", " + opt)
			error("cqi cache miss on calibration params for variant, opt: " + variant + ", " + opt)
		}
		res
	}

	private def calibrationParamsMap : scala.collection.mutable.HashMap[String, CalibrationParams] = {
		PermaCacher.getOrRegister("userfeedback-content-quality-calibration-params-cache", {
			var processCnt = 0
			var lineCnt = 0
			val cacheMap = new scala.collection.mutable.HashMap[String, CalibrationParams]()

			try{
				grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, calibrationParamHdfsPath) { line =>
					line.split('^') match {
						case Array(userfeedbackvariationid,chosenuserfeedbackoptionid,calibratenumeric,calibratepositivity,calibratenegativity) => {
							try{
								val variant = userfeedbackvariationid.toInt
								val opt = chosenuserfeedbackoptionid.toInt
								val numeric = calibratenumeric.toDouble
								val positivity = calibratepositivity.toDouble
								val negativity = calibratenegativity.toDouble
								val key = getKey(variant, opt)
								val calibrationParams = CalibrationParams(numeric, positivity, negativity)
								cacheMap.put(key, calibrationParams)
								processCnt += 1
							}
							catch{
								case ex: Exception =>
									warn(ex, "Failed to parse line in " + calibrationParamHdfsPath + ".  Line: " + line)
							}
						}
						case _ =>
							warn("Failed to parse line in " + calibrationParamHdfsPath + ".  Line: " + line)

					}

					lineCnt+=1
				}
			}
			catch {
				case ex: Exception =>
					warn(ex, s"Failed to load " + calibrationParamHdfsPath + ".  User Feedback Content Quality Computation will Fail.")
			}

			info("loaded " + processCnt + " items into cache from " + calibrationParamHdfsPath)
			cacheMap
		}, reloadFrequencySeconds)
	}

	private def normailizationParamsMap : scala.collection.mutable.HashMap[String, NormaliziationParams] = {
		PermaCacher.getOrRegister("userfeedback-content-quality-normalization-params-cache", {
			var processCnt = 0
			var lineCnt = 0
			val cacheMap = new scala.collection.mutable.HashMap[String, NormaliziationParams]()

			try{
				grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, normalizationParamHdfsPath) { line =>
					line.split('^') match {
						case Array(userfeedbackvariationid,chosenuserfeedbackoptionid,p,theta) => {
              try{
								val variant = userfeedbackvariationid.toInt
								val opt = chosenuserfeedbackoptionid.toInt
								val pDouble = p.toDouble
								val thetaDouble = theta.toDouble
								val key = getKey(variant, opt)
								val pAndTheta = NormaliziationParams(pDouble, thetaDouble)
								cacheMap.put(key, pAndTheta)
								processCnt += 1
							}
							catch{
								case ex: Exception =>
									warn(ex, "Failed to parse line in " + normalizationParamHdfsPath + ".  Line: " + line)
								}
							}
						case _ =>
							warn("Failed to parse line in " + normalizationParamHdfsPath + ".  Line: " + line)

					}

					lineCnt +=1
				}
			}
			catch {
				case ex: Exception =>
					warn(ex, s"Failed to load " + normalizationParamHdfsPath + ".  User Feedback Content Quality Computation will Fail.")
			}

			info("loaded " + processCnt + " items into cache from " + normalizationParamHdfsPath)

			cacheMap
		}, reloadFrequencySeconds)
  }

	def getKey(variant: Int, opt: Int) = variant + "-" + opt

	private def getValidKeys() = {

		PermaCacher.getOrRegister("userfeedback-content-quality-valid-param-keys", {


			val calibrationKeys = calibrationParamsMap.keySet
			val normalizationKeys = normailizationParamsMap.keySet

			val commonKeys = calibrationKeys.intersect(normalizationKeys)

			if (commonKeys.size != calibrationKeys.size || commonKeys.size != normalizationKeys.size) {
				error("Content Quality Index calibration and normalization params not consistent.  calibrationKeys: " + calibrationKeys.size + ", normalizationKeys: " + normalizationKeys.size + ", commonKeys: " + commonKeys.size)

				val onlyCalibrationKeys = calibrationKeys -- normalizationKeys
				val onlyNormalizationKeys = normalizationKeys -- calibrationKeys

				error("onlyCalibrationKeys: " + onlyCalibrationKeys)
				error("onlyNormalizationKeys: " + onlyNormalizationKeys)

			}

			commonKeys
		}, reloadFrequencySeconds)

	}
}
