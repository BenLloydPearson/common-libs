package com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.doc2vec

import com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

import scala.collection._

/*
 *    __   _         __
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, /
 *                       /___/
 *
 * Trait that defines a contract to compute a doc score from a seq of term scores for the doc
 *
 */

trait DocVectorizer[Doc] extends Serializable {
	def apply(doc: Doc, termVectors: Iterable[LabeledPoint[String]]): Option[Vector]
}
