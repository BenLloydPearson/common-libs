package com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib

import org.apache.spark.mllib.linalg._
import com.gravity.interests.jobs.intelligence.helpers.grvspark._

import scala.collection._
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Container class for a user-defined item and it's feature vector
 * It differs from MLLib's LabeledPoint in that it supports any type as the label
 */
case class LabeledPoint[T](value: T, features: Vector) {
	def tuple: (T, Vector) = value -> features
}

object LabeledPoint {
	implicit def toMlLib(p: LabeledPoint[Double]): org.apache.spark.mllib.regression.LabeledPoint = org.apache.spark.mllib.regression.LabeledPoint(p.value, p.features)
	implicit def toBreezeVector(p: LabeledPoint[Double]): breeze.linalg.Vector[Double] = p.features
	implicit def toMlVector(p: LabeledPoint[Double]): org.apache.spark.mllib.linalg.Vector = p.features
}
