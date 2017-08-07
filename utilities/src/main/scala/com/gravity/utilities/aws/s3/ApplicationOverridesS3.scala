package com.gravity.utilities.aws.s3

import java.io._
import java.util.Properties

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.gravity.utilities.{Settings, SystemPropertyHelpers}

/**
 * Created by agrealish14 on 6/28/15.
 */
object ApplicationOverridesS3 {
 import com.gravity.logging.Logging._
  private lazy val properties = {
    val props: Properties = new Properties


    try {

      val settingsFile = SystemPropertyHelpers.settingsFileName

      props.load(classOf[Settings].getResourceAsStream("/" + settingsFile))

      props
    }
    catch {
      case e: IOException => {
        throw new RuntimeException(e)
      }
    }
  }

  private lazy val awsCredentials = {
    val awsAccessKey = properties.getProperty("grv-app-overrides.amazon.key.access")
    val awsSecretKey = properties.getProperty("grv-app-overrides.amazon.key.secret")

    new BasicAWSCredentials(awsAccessKey, awsSecretKey)
  }

  val overridesFilename = "override.properties"
  val assetsManifestFilename = "assets.manifest"

  val bucket: String = properties.getProperty("grv-app-overrides.amazon.bucket")
  val dir: String = {
    SystemPropertyHelpers.environmentProperty
  }

  val role: String = {
    SystemPropertyHelpers.roleProperty
  }

  def s3Client: AmazonS3Client = new AmazonS3Client(awsCredentials)

  def overridesFile(): InputStream = {

    try {

      val s3Object = s3Client.getObject(bucket, adjustKeyForS3(overridesFilename))

      s3Object.getObjectContent

    } catch {
      case t: Throwable =>
        warn(t.getMessage)
        throw t
    }

  }

  def assetsManifest(): List[String] = {

    try {

      val fileLines = collection.mutable.ArrayBuffer[String]()

      val s3Object = s3Client.getObject(bucket, dir+"/"+assetsManifestFilename)

      warn("Fetching assets.manifest from S3 bucket: " + bucket + " key: " + dir+"/"+assetsManifestFilename)

      val reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent))
      var line = reader.readLine()
      while(line != null) {

        fileLines += line

        line = reader.readLine()
      }

      fileLines.toList

    } catch {
      case t: Throwable =>
        warn(t.getMessage)
        throw t
    }
  }

  def adjustKeyForS3(key:String): String = {

    val s3Key = dir+"/"+role+"."+key

    warn("Fetching override.properties from S3 bucket: " + bucket + " key: " + s3Key)

    s3Key
  }

}

