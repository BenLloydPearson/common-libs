package com.gravity.interests.jobs.intelligence.schemas

import java.io._
import java.util.zip.GZIPInputStream
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectMetadata, S3ObjectInputStream}
import com.google.common.base.Charsets
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.{ArchiveUtils, Settings}
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import org.apache.commons.io.IOUtils

import scalaz.Scalaz._
import scalaz._

/**
 * Created by agrealish14 on 2/12/15.
 */
case class DefaultRecosS3UpdateFailure(t: Throwable) extends FailureResult("Could not update default recos on S3", Some(t))

case class DefaultRecosS3FetchFailure(t: Throwable, key:String) extends FailureResult(s"Could not fetch default recos from S3 for key: $key", None)

object RecommendationsS3 {
 import com.gravity.logging.Logging._

  private lazy val awsCredentials = {
    val awsAccessKey = Settings.getProperty("grv-recogen.amazon.key.access")
    val awsSecretKey = Settings.getProperty("grv-recogen.amazon.key.secret")

    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  lazy val bucket: String = Settings.getProperty("grv-recogen.amazon.bucket")
  val dir: String = Settings.getProperty("grv-recogen.amazon.directory")

  def s3Client: AmazonS3Client = new AmazonS3Client(awsCredentials)

  def get(key: String): Validation[FailureResult, String] = {

    val s3Key = adjustKeyForS3(key)

    try {

      val s3Object = s3Client.getObject(bucket, dir+"/"+s3Key)

      val content: S3ObjectInputStream = s3Object.getObjectContent

      val gzip = new GZIPInputStream(content)
      val json = try {
        IOUtils.toString(gzip, Charsets.UTF_8.name())
      } finally {
        gzip.close()
      }

      Some(json) toSuccess FailureResult(s"Fallback not found for fallback key: $key")

    }
    catch {
      case ase:AmazonServiceException =>
        warn("Error Message:" + ase.getMessage +
          " HTTP Status Code: " + ase.getStatusCode +
          " AWS Error Code: " + ase.getErrorCode +
          " Error Type: " + ase.getErrorType +
          " Request ID: " + ase.getRequestId +
          " key: " + s3Key)
        DefaultRecosS3FetchFailure(ase, key).failure
      case ace:AmazonServiceException =>
        warn("Error Message: " + ace.getMessage +
          " key: " + s3Key)
        DefaultRecosS3FetchFailure(ace, key).failure
      case t: Throwable =>
        warn("Error Message: " + t.getMessage +
          " key: " + s3Key)
        DefaultRecosS3FetchFailure(t, key).failure

    }

  }

  def upsertKeyValue(key: String, json: String): ValidationNel[FailureResult, Unit] = {

    val s3Key = adjustKeyForS3(key)

    if(HBaseConfProvider.isUnitTest) { // no upload to s3 for unit tests
      return unitSuccessNel
    }

    try {

      val metadata = new ObjectMetadata()
      metadata.setContentType("application/json")
      metadata.setContentEncoding("gzip")

      val bytes: Array[Byte] = ArchiveUtils.compressBytes(json.getBytes("UTF-8"), withHeader = true)
      val contentLength = bytes.length.toLong
      metadata.setContentLength(contentLength)

      try {
        s3Client.putObject(bucket, dir+"/"+s3Key, new ByteArrayInputStream(bytes), metadata)
      }
      catch {
        case ase:AmazonServiceException =>
          warn("Error Message:" + ase.getMessage +
            " HTTP Status Code: " + ase.getStatusCode +
            " AWS Error Code: " + ase.getErrorCode +
            " Error Type: " + ase.getErrorType +
            " Request ID: " + ase.getRequestId +
            " key: " + s3Key)
          throw ase
        case ace:AmazonServiceException =>
          warn("Error Message: " + ace.getMessage +
            " key: " + s3Key)
          throw ace
        case t: Throwable =>
          warn("Error Message: " + t.getMessage +
            " key: " + s3Key)
          warn(DefaultRecosS3UpdateFailure(t))
          throw t
      }
    }
    catch {
      case t: Throwable => DefaultRecosS3UpdateFailure(t).failureNel[Unit]
    }

    unitSuccessNel
  }

  def adjustKeyForS3(key:String): String = {
    key.replaceAll(" ", "") + ".json"
  }
}

