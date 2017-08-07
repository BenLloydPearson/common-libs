package com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib

import com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib.doc2vec.{DocVectorizer, DocVectorizerAverageTerms}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection._
import scala.reflect.ClassTag
/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */
class MLLibSparkContextFunctions(sc: SparkContext) {

	implicit def tfIdf[Doc : ClassTag, Term : ClassTag](input: RDD[Doc], docToTerms: Doc => Iterable[Term], minDocCount: Int = 0): RDD[(Doc, Iterable[(Term, Double)])] = {
		val hashingTF = new HashingTF()
		val termFrequencies = input.map(doc => (doc, hashingTF.transform(docToTerms(doc))))

		val idf = new IDF(minDocCount).fit(termFrequencies.map(_._2))
		val bcIdf = sc.broadcast(idf)

		termFrequencies.mapPartitions(_.map{ case (doc, v) => {
			val scaled = bcIdf.value.transform(v)
			doc -> docToTerms(doc).map(t => t -> scaled(hashingTF.indexOf(t)))
		}})
	}

	/*
		doc2vec algorithm that creates doc vectors by mapping each document to a sequence of sentences, each of which
		contains a sequence of terms.  For each term we do a lookup for the term's vector in the model, which may not exist.

		For all term vectors found in the model, we pass them to the 'docScorer' to compute a doc vector.
	 */
	implicit def doc2vec[Doc : ClassTag](input: RDD[Doc], docToSentences: Doc => Iterable[Iterable[String]], model: Word2VecModel, docVectorizer: DocVectorizer[Doc] = new DocVectorizerAverageTerms()): RDD[(Doc, Option[Vector])] = {
		// broadcast the vocabulary
		val vocab = sc.broadcast(model.getVectors)

		try {
			// for each doc, gather the term vectors, pass them to the DocScorer to compute the doc vector
			input.mapPartitions(_.map(doc => {
				doc -> docVectorizer(doc, docToSentences(doc).flatMap(terms => terms.flatMap(term => vocab.value.get(term).map(v => LabeledPoint[String](term, Vectors.dense(v.map(_.toDouble)))).toSeq)))
			}))
		} finally {
			vocab.unpersist()
		}
	}
}
