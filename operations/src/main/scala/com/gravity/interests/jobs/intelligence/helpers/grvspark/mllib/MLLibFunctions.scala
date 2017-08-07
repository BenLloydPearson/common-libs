package com.gravity.interests.jobs.intelligence.helpers.grvspark.mllib

import org.apache.spark.mllib.linalg._
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}
import org.elasticsearch.common.recycler.Recycler.V

import scala.collection._

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 * Implicit conversions for Breeze <-> Mllib for vectors and matrices
 * Under the hood, Mllib uses Breeze but keeps it encapsulated-- so these conversions allow
 * us to expose the breeze API when working with Mllib
 */
trait MLLibFunctions extends LowPriorityMLLibFunctions {

	// implicit conversions for Mllib -> Breeze data types (specific Sparse/Dense versions)
	implicit def mlDenseVectorToBreeze[V <: DenseVector](v: V): BDV[Double] = new BDV[Double](v.toArray)
	implicit def mlSparseVectorToBreeze[V <: SparseVector](v: V): BSV[Double] = new BSV[Double](v.indices, v.values, v.size)
	implicit def mlDenseMatrixToBreeze[V <: DenseMatrix](v: V): BDM[Double] = toBreeze(v).asInstanceOf[BDM[Double]]
	implicit def mlSparseMatrixToBreeze[V <: SparseMatrix](v: V): BSM[Double] = toBreeze(v).asInstanceOf[BSM[Double]]

	// implicit conversions for Breeze -> Mllib data types (specific Sparse/Dense versions)
	implicit def breezeDenseVectorToMl[V <: BDV[Double]](v: V): DenseVector = new DenseVector(v.data)
	implicit def breezeSparseVectorToMl[V <: BSV[Double]](v: V): SparseVector = new SparseVector(v.length, v.index, v.data)
	implicit def breezeDenseMatrixToMl[V <: BDM[Double]](v: V): DenseMatrix = toBreeze(v).asInstanceOf[DenseMatrix]
	implicit def breezeSparseMatrixToMl[V <: BSM[Double]](v: V): SparseMatrix = toBreeze(v).asInstanceOf[SparseMatrix]

	// convert any scala collection of Double's to a Mllib Dense vector
	implicit def collectionDoubleToMlVector[T <: Traversable[Double], V](col: T): DenseVector = Vectors.dense(col.toArray).asInstanceOf[DenseVector]

}

trait LowPriorityMLLibFunctions {

	// convert any scala collection of numeric type to a Mllib Dense vector
	implicit def collectionToMlVector[T[+x] <: Traversable[x], V](col: T[V])(implicit n: Numeric[V]): DenseVector = Vectors.dense(col.map(v => n.toDouble(v)).toArray).asInstanceOf[DenseVector]

	/**
	 * implicit conversions for Mllib -> Breeze data types (generic conversion)
	 */
	implicit def toBreeze(vector: Vector): BV[Double] = vector match {
		case v: DenseVector => new BDV[Double](v.values)
		case v: SparseVector => new BSV[Double](v.indices, v.values, v.size)
	}

	implicit def toBreeze(matrix: Matrix): BM[Double] = matrix match {
		case v: DenseMatrix =>
			if (!v.isTransposed) {
				new BDM[Double](v.numRows, v.numCols, v.values)
			} else {
				val breezeMatrix = new BDM[Double](v.numCols, v.numRows, v.values)
				breezeMatrix.t
			}
		case v: SparseMatrix =>
			if (!v.isTransposed) {
				new BSM[Double](v.values, v.numRows, v.numCols, v.colPtrs, v.rowIndices)
			} else {
				val breezeMatrix = new BSM[Double](v.values, v.numRows, v.numCols, v.colPtrs, v.rowIndices)
				breezeMatrix.t
			}
	}

	/**
	 * implicit conversions for Breeze -> Mllib data types (generic types)
	 */

	implicit def fromBreeze(breezeVector: BV[Double]): Vector = breezeVector match {
		case v: BDV[Double] =>
			if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
				new DenseVector(v.data)
			} else {
				new DenseVector(v.toArray)  // Can't use underlying array directly, so make a new one
			}
		case v: BSV[Double] =>
			if (v.index.length == v.used) {
				new SparseVector(v.length, v.index, v.data)
			} else {
				new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
			}
		case v: BV[_] =>
			sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
	}


	implicit def fromBreeze(breeze: BM[Double]): Matrix = breeze match {
		case dm: BDM[Double] =>
			new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
		case sm: BSM[Double] =>
			// Spark-11507. work around breeze issue 479.
			val mat = if (sm.colPtrs.last != sm.data.length) {
				val matCopy = sm.copy
				matCopy.compact()
				matCopy
			} else {
				sm
			}
			// There is no isTranspose flag for sparse matrices in Breeze
			new SparseMatrix(mat.rows, mat.cols, mat.colPtrs, mat.rowIndices, mat.data)
		case _ =>
			throw new UnsupportedOperationException(
				s"Do not support conversion from type ${breeze.getClass.getName}.")
	}
}
