package com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.doc2vec

import org.apache.spark.mllib.linalg.Vector
import com.gravity.interests.jobs.intelligence.helpers.grvspark._

import scala.collection._
import breeze.linalg._
import breeze.math._
import breeze.stats._
import com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.LabeledPoint

import scala.Seq
/**
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Computes a doc score by averaging the term vectors
 *
 * @param minVectors required number of term vectors to be present to return a score
 *                   (ie: minVectors = 5 will only score documents that have at least 5 term vectors available)
 */
class DocVectorizerAverageTerms[Doc](minVectors: Int = 1) extends DocVectorizer[Doc] {

	override def apply(doc: Doc, termVectors: Iterable[LabeledPoint[String]]): Option[Vector] = {
		if (termVectors.size >= minVectors) {
			val vectors = breeze.linalg.DenseMatrix(termVectors.map(_.features.toArray).toSeq: _*)
			Option[Vector](mean(vectors(::, *)).toDenseVector)
		} else {
			None
		}
	}

}
