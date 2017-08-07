package com.gravity.interests.jobs.intelligence.operations


import java.io.{ObjectOutputStream, ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream}
import com.gravity.interests.jobs.intelligence._
import com.gravity.service.remoteoperations.{MergeableResponse, SplittablePayload}
import com.gravity.utilities.VariableLengthEncoding
import com.gravity.utilities.components.FailureResult

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Set, Map}
import scalaz._, Scalaz._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/11/15
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

@SerialVersionUID(-5211144295189460759l)
case class ArticleDataLiteResponse(byteses: Seq[Array[Byte]]) extends MergeableResponse[ArticleDataLiteResponse] {

  @transient lazy val validations = getValidations

  def size = {
    var count = 0
    validations.foreach(validation => {
      validation.foreach{map => count += map.size}
    })
    count
  }

  override def merge(withThese: Seq[ArticleDataLiteResponse]): ArticleDataLiteResponse = {
    val allArrays = new ArrayBuffer[Array[Byte]]()
    allArrays.appendAll(byteses)
    withThese.foreach(other => allArrays.appendAll(other.byteses))
    ArticleDataLiteResponse(allArrays.toSeq)
  }

  def getValidations : Seq[ValidationNel[FailureResult, Map[ArticleKey, Validation[FailureResult, ArticleRow]]]] = {
    byteses.map(getValidation)
  }

  def getValidation(bytes: Array[Byte]) : ValidationNel[FailureResult, Map[ArticleKey, Validation[FailureResult, ArticleRow]]] = {
    val bis = new ByteArrayInputStream(bytes)
    val success = 1 == bis.read()
    if(success) {
      val mapSize = VariableLengthEncoding.readIntFromStream(bis)
      val keyBytes = new Array[Byte](8 * mapSize)
      bis.read(keyBytes)
      val keys = {for(i <- 0 until mapSize) yield ArticleDataLiteResponse.bytesToLong(keyBytes, i * 8)}.map(ArticleKey(_))
      val valueValidations = for(i <- 0 until mapSize) yield {
        val size = VariableLengthEncoding.readIntFromStream(bis)
        val nextArray = new Array[Byte](size)
        bis.read(nextArray)
        if(nextArray.nonEmpty) {
          Schema.Articles.cache.asInstanceOf[IntelligenceSchemaCacher[ArticlesTable, ArticleKey, ArticleRow]].fromBytesTransformer(nextArray)
        }
        else {
          FailureResult("Row was unable to be serialized").failure
        }
      }
      keys.zip(valueValidations).toMap.successNel
    }
    else {
      val ois = new ObjectInputStream(bis)
      val fails = ois.readObject().asInstanceOf[List[FailureResult]].toNel.get
      fails.failure[Map[ArticleKey, Validation[FailureResult, ArticleRow]]]
    }
  }
}

object ArticleDataLiteResponse {

  def apply(validation: ValidationNel[FailureResult, Map[ArticleKey, Array[Byte]]]) : ArticleDataLiteResponse = {
    val bos = new ByteArrayOutputStream()

    validation match {
      case Success(resultMap) => //1 for success, number of items, each key as a full long, series of encoded byte lengths + row bytes
        bos.write(1)
        bos.write(VariableLengthEncoding.encode(resultMap.size))
        resultMap.keys.foreach(key => bos.write(longToBytes(key.articleId)))
        resultMap.values.foreach(value => {
          bos.write(VariableLengthEncoding.encode(value.length))
          bos.write(value)
        })
      case Failure(fails) =>
        bos.write(0)
        val out = new ObjectOutputStream(bos)
        out.writeObject(fails.list)
        out.close()
    }

    ArticleDataLiteResponse(Seq(bos.toByteArray))
  }

  private def longToBytes(v: Long) : Array[Byte] = {
    val bytes = new Array[Byte](8)
    bytes(0) = (v >>> 56).toByte
    bytes(1) = (v >>> 48).toByte
    bytes(2) = (v >>> 40).toByte
    bytes(3) = (v >>> 32).toByte
    bytes(4) = (v >>> 24).toByte
    bytes(5) = (v >>> 16).toByte
    bytes(6) = (v >>> 8).toByte
    bytes(7) = (v >>> 0).toByte
    bytes
  }

  protected[operations] def bytesToLong(bytes: Array[Byte], startIndex: Int) : Long = {
    (bytes(startIndex + 0).toLong << 56) +
      ((bytes(startIndex + 1) & 255).toLong << 48) +
      ((bytes(startIndex + 2) & 255).toLong << 40) +
      ((bytes(startIndex + 3) & 255).toLong << 32) +
      ((bytes(startIndex + 4) & 255).toLong << 24) +
      ((bytes(startIndex + 5) & 255) << 16) +
      ((bytes(startIndex + 6) & 255) << 8) +
      ((bytes(startIndex + 7) & 255) << 0)
  }
}

case class ArticleDataLiteRequest(keys: Set[ArticleKey]) extends SplittablePayload[ArticleDataLiteRequest] {
  def size = keys.size

  override def split(into: Int): Array[Option[ArticleDataLiteRequest]] = {
    val results = new Array[scala.collection.mutable.Set[Long]](into)

    keys.foreach {key => {
      val thisIndex = bucketIndexFor(key.articleId, into)
      if(results(thisIndex) == null)
        results(thisIndex) = scala.collection.mutable.Set[Long](key.articleId)
      else
        results(thisIndex).add(key.articleId)
    }}

    results.map{result => {
      if(result == null)
        None
      else
        Some(ArticleDataLiteRequest(result.map(ArticleKey(_))))
    }}
  }


}