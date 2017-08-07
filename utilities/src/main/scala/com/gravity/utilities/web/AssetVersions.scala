package com.gravity.utilities.web

import java.io.File
import java.nio.charset.Charset
import javax.servlet.http.HttpServletRequest

import com.gravity.utilities.analytics.URLUtils
import com.gravity.utilities.aws.s3.ApplicationOverridesS3
import com.gravity.utilities.cache.PermaCacher
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.web.http._
import com.gravity.utilities.{HasGravityRoleProperties, Settings2}

import scala.collection._
import scala.io.Codec
import scalaz.syntax.std.option._

/**
 * Lookup the latest version of static assets according to the version in their filename, if any.
 * Always use this class to access static assets rather than hardcoding them in HTML for cache-busting,
 * picking up the correct version from a central asset server, and transparently sourcing local assets
 * during development.
 */
trait AssetVersions {
  import com.gravity.utilities.Counters._

  /**
   * Finds the latest version of a base, unversioned asset filepath, returning its path with the CDN asset prefix.
   * If the file version isn't known, returns the unversioned asset filepath with the CDN asset prefix.
   *
   * For example, if the only files in the servlet are {"/personalization/base.1.0.2.css", "/personalization/base.1.0.3.css",
   * "/personalization/logo.png"}, latestPrefixedAssetUrl("/personalization/base.css") will use base.1.0.3.css, returning
   * something like "http://cdn.gravity.com/personalization/base.1.0.3.css".
   * latestPrefixedAssetUrl("/personalization/logo.png") would then return the unversioned logo.png,
   * "http://cdn.gravity.com/personalization/logo.png".
   *
   * @param eagerCacheBust TRUE to cause cache busting of the asset based on current system time. This is not production
   *                       performant and intended only as a convenience for local dev.
   * @param forceSecureUrl Normally secure URL is determined by the implicit $request param; however, you can use this to
   *                       force use of secure URL.
   */
  def latestPrefixedAssetUrl(filePath: String, eagerCacheBust: Boolean = false, forceSecureUrl: Boolean = false)(implicit request: HttpServletRequest): String = {
    val assetFilePath = latestAssetIfBundled(filePath) getOrElse filePath
    val assetUrl = assetPrefix(forceSecureUrl) + assetFilePath
    if (eagerCacheBust) {
      URLUtils.appendParameter(assetUrl, "cacheBust=" + System.currentTimeMillis())
    } else {
      assetUrl
    }
  }

  def latestAssetIfBundled(filePath: String): Option[String]

  /**
   * @param forceSecureUrl Normally secure URL is determined by the implicit $request param; however, you can use this to
   *                       force use of secure URL.
   */
  def assetPrefix(forceSecureUrl: Boolean = false)(implicit request: HttpServletRequest): String = {
    if(forceSecureUrl || (
      request.getServerName == "localhost" && request.isSecure
    )) {
      countPerSecond("SSL","SSL Asset Requests")
      AssetVersions.secureAssetPrefixSetting getOrElse request.getRequestUrlToContextPath
    }
    else {
      countPerSecond("SSL","Non-SSL Asset Requests")
      AssetVersions.assetPrefixSetting getOrElse request.getRequestUrlToContextPath
    }
  }
}

object AssetVersions extends HasGravityRoleProperties {
  private val assetPrefixSetting = Option(properties.getProperty("assets.prefix")) match {
    case somePrefix @ Some(prefix) if prefix.nonEmpty => somePrefix
    case _ => None
  }
  private val secureAssetPrefixSetting = Option(properties.getProperty("assets.prefix.secure")) match {
    case somePrefix @ Some(prefix) if prefix.nonEmpty => somePrefix
    case _ => None
  }
}

/**
 * Reads assets from a Gravity Ops manifest file, in the map format:
 *
 *  /foo/bar/baz.css|/foo/bar/baz.01234567deadbeef.css
 *  /foo/bar/3/index.js|/foo/bar/3/index.89abcdef0123456.js
 */
trait AssetVersionsFromOpsManifest extends AssetVersions with HasGravityRoleProperties {
  import com.gravity.logging.Logging._

  case class EmptyAssetManifest(ex: Exception) extends FailureResult("output of asset manifest was empty map", Some(ex))
  case class NoAssetManifestProperty(ex: Exception) extends FailureResult("No assets.manifest property defined", Some(ex))

  protected def manifestLines: TraversableOnce[String]

  protected def filesToVersionedFiles: Map[String, String] = {
    import scala.collection.TraversableOnce._ // override Scalaz implicit trying to take over
    val pairs = for {
      line <- manifestLines
      (filePath, versionedFilePath) <- line.trim.split("\\|", 2) match {
        case Array(filePath, versionedFilePath) => (filePath, versionedFilePath).some
        case _ => None
      }
    } yield filePath -> versionedFilePath

    val map = pairs.toMap

    if(map == Map.empty && showAssetManifestErrorTraces) {
      trace(EmptyAssetManifest(new IllegalStateException))  //Lowered to trace as the empty asset manifest problem has not appeared recently
    }

    map
  }

  def latestAssetIfBundled(filePath: String): Option[String] = filesToVersionedFiles.get(filePath)

  protected lazy val showAssetManifestErrorTraces = Option(properties.getProperty("assets.showAssetManifestErrorTraces")).flatMap(_.tryToBoolean).getOrElse(true)
}

object AssetVersionsFromOpsManifest extends AssetVersionsFromOpsManifest {
 import com.gravity.logging.Logging._
  implicit val codec: Codec = new Codec(Charset forName "UTF-8")

  private def manifestFile = {
    val manifestFilePathOption = Option(properties.getProperty("assets.manifest"))
    if(manifestFilePathOption.isEmpty && showAssetManifestErrorTraces){
      trace(NoAssetManifestProperty(new IllegalStateException))
    }
    val fileOption = for {
      filePath <- manifestFilePathOption
      f = new File(filePath)
      if f.length > 0
    } yield f

    if (manifestFilePathOption.isDefined) {
      fileOption match {
        case Some(file) => info("assets.manifest defined in " + manifestFilePathOption)
        case None => error("assets.manifest defined as " + manifestFilePathOption + " but was empty")
      }
    }

    fileOption
  }

  override protected def manifestLines = for {
    line <- fetchManifestLinesList()
  } yield line


  private def fetchManifestLinesList(): List[String] = {

    val isAws= System.getProperty("com.gravity.settings.aws", "false").equals("true")
    if(isAws) {

      ApplicationOverridesS3.assetsManifest()

    } else {

      manifestFile match {
        case Some(file) =>
          val source = scala.io.Source.fromFile(manifestFile.get)
          try {

            source.getLines().toList

          } finally {
            source.close()
          }

        case None =>
          if(Settings2.roleRequiresAssetsManifest(Settings2.role))
            warn("assets.manifest not found")

          List.empty[String]
      }

    }
  }


  override protected def filesToVersionedFiles = {
    def work = super.filesToVersionedFiles

    if(cacheAssetManifest)
      PermaCacher.getOrRegister("AssetVersionsFromOpsManifest", reloadInSeconds = 60 * 5, mayBeEvicted = false)(work)
    else
      work
  }

  protected lazy val cacheAssetManifest = Option(properties.getProperty("assets.manifest.cache")).flatMap(_.tryToBoolean).getOrElse(true)
}