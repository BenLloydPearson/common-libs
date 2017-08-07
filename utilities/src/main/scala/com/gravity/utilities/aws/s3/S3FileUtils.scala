package com.gravity.utilities.aws.s3

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{S3ObjectSummary, ListObjectsRequest, DeleteObjectsRequest}
import com.gravity.utilities.Settings
import scala.collection.JavaConversions._

/** Helpful class for file/folder type functionality against s3
  *
  * Created by jengelman14 on 2/6/17.
  *
  * @constructor Create a new connection instance
  * @param settingsPrefix the prefix of the setting name before ".key.access" and ".key.secret" to
  *                       get the credentials for connecting to s3
  */
sealed case class S3FileUtils(settingsPrefix: String) {
  class BucketDoesNotExistException(bucket: String) extends Exception("Bucket does not exist.")

  private lazy val awsCredentials = {
    val awsAccessKey = Settings.getProperty(settingsPrefix + ".key.access")
    val awsSecretKey = Settings.getProperty(settingsPrefix + ".key.secret")

    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  lazy val s3Client: AmazonS3Client = new AmazonS3Client(awsCredentials)

  def bucketExists(bucket: String): Boolean = {
    s3Client.doesBucketExist(bucket)
  }

  /** Checks to see if any files exist in the given bucket with the given prefix
    *
    * @param bucket
    * @param prefix
    * @return boolean
    */
  def objectExists(bucket: String, prefix: String): Boolean = {
    if (bucketExists(bucket)) {
      val objs = s3Client.listObjects(bucket, prefix).getObjectSummaries
      if (objs.size == 0) {
        false
      } else {
        true
      }
    } else {
      throw new BucketDoesNotExistException(bucket)
    }
  }

  /** Gets a page of object data on s3 in the given bucket with the given prefix
    *
    * @param bucket
    * @param prefix
    * @param marker optional page marker representing the page to get, None for first page
    * @return tuple of (hasMorePages: Boolean, nextPageMarker: Option[String], objectSummaries: List)
    */
  def listObjectsPaged(bucket: String, prefix: String, marker: Option[String] = None) = {
    val req = marker match {
      case Some(x) => new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix).withMarker(x)
      case _ => new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix)
    }
    val lst = s3Client.listObjects(req)
    (lst.isTruncated, Some(lst.getNextMarker), lst.getObjectSummaries.toList)
  }
  def listAllObjects(bucket: String, prefix: String) = {
    var continue = false
    var nextMarker: Option[String] = None
    var outObjects: List[S3ObjectSummary] = List.empty[S3ObjectSummary]
    if (bucketExists(bucket)) {
      do {
        listObjectsPaged(bucket, prefix, nextMarker) match {
          case (hasMore, marker, objects) => {
            continue = hasMore
            nextMarker = marker
            outObjects = outObjects ++ objects
          }
        }
      } while (continue)
    } else {
      throw new BucketDoesNotExistException(bucket)
    }
    outObjects
  }

  def deleteObject(bucket: String, key: String) = {
    s3Client.deleteObject(bucket, key)
  }

  /** Deletes all objects in the corresponding bucket with the provided prefix
    *
    * @param bucket
    * @param prefix
    * @return the number of objects deleted
    */
  def deleteObjects(bucket: String, prefix: String): Int = {
    if (prefix == null || prefix.trim.length < 2) {
      throw new Exception("Prefix must be at least 2 characters long, trying not to let you accidentally delete too much here...")
    }
    if (bucketExists(bucket)) {
      val batch_size = 128
      val objs = listAllObjects(bucket, prefix)
      var window_start = 0
      var window_end = batch_size

      do {
        val req = new DeleteObjectsRequest(bucket)
          .withBucketName(bucket)
          .withKeys(objs.slice(window_start, window_end).map(x => x.getKey): _*)

        s3Client.deleteObjects(req)

        window_start = window_end
        window_end = window_start + batch_size

      } while (window_end <= objs.length)

      objs.size
    } else {
      throw new BucketDoesNotExistException(bucket)
    }
  }

  /** Calls shutdown on the s3 client
    *
    * This is an optional method, and callers are not expected to call it, but can if they want to explicitly release any open resources.
    * Once destroyed, it should not be used to make any more requests.
    */
  def destroy() = s3Client.shutdown()
}
