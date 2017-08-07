package com.gravity.interests.jobs.intelligence.operations

import java.net.{URL, URI}
import scala.util.{Try, Success, Failure}
import java.net.URLDecoder

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.Schema
import com.gravity.interests.jobs.intelligence.helpers.grvcascading
import org.apache.hadoop.fs.{FileUtil, Path}

/**
 * Emit the Image URLs that contain illegal characters.
 */
class IllegalImageURLsJob(outputDirectory: String, combineOutput: Boolean = true) extends Serializable {
  import grvcascading._

  val fldNames = List("md5RowKey", "originalImage", "encodedImage", "httpStatus", "errorMsg", "cachedImage")

  val counterGroup = "Custom - IllegalImageURLsJob"

  val source       = grvcascading.fromTable(Schema.CachedImages)(_.withAllColumns)
  val destination  = grvcascading.toTextDelimited(outputDirectory)(fldNames: _*)

  val asciiControlCharacters = """[\x00-\x1F\x7F]""".r.unanchored
  val nonAsciiCharacters = """[\x80-\xFF]""".r.unanchored
  //unsafeCharacters listed that seem to be fine: ~
  val unsafeCharacters = """[ "<>#{}|\\^\[\]]`""".r.unanchored



  val assembly = grvcascading.pipe("Map Images in IllegalImageURLsJob").each((flowProcess, call) => {
    flowProcess.increment(counterGroup, "CachedImages Read", 1)

    //for now we're just checking the PATH for:
    //         ascii control characters, non-ascii characters and unsafe characters
    // as per https://perishablepress.com/stop-using-unsafe-characters-in-urls/
    // NOTE: we're not checking for reserved characters at the moment
    def imagePathIsIllegal(urlString: String): Boolean = {
      Try(new URL(urlString)) match {
        case Failure(_) =>
          flowProcess.increment(counterGroup, "Image URL() failed", 1)
          true
        case Success(url) =>
          url.getPath match {
            case asciiControlCharacters(_*) =>
              flowProcess.increment(counterGroup, "Ascii Control Characters", 1)
              true
            case nonAsciiCharacters(_*) =>
              flowProcess.increment(counterGroup, "Non-Ascii Characters", 1)
              true
            case unsafeCharacters(_*) =>
              flowProcess.increment(counterGroup, "Unsafe Characters", 1)
              true
            case _ => false
          }
      }
    }

    def sanitizeUrlString(urlString: String): Option[String] = {
      try {
        val url = new URL( URLDecoder.decode(urlString, "UTF-8") )
        flowProcess.increment(counterGroup, "Able to create URL", 1)

        val uri = new URI(url.getProtocol, url.getAuthority, url.getPath, url.getQuery, null)
        flowProcess.increment(counterGroup, "Able to create URI", 1)

        Some(uri.toURL.toString)
      } catch {
        case e: Throwable => None
      }
    }

    def urlStringForCompare(urlString: String): String = {
      urlString.map {
        //these are the reserved keywords that are only encoded outside their defined usage
        //that said, we don't care where they're used since servers can be pretty lenient
        case '$' => "%24"
        case '&' => "%26"
        case '+' => "%20" //space and + are interchangable
        case ',' => "%2C"
        case '/' => "%2F"
        case ':' => "%3A"
        case ';' => "%3B"
        case '=' => "%3D"
        case '?' => "%3F"
        case '@' => "%40"
        case c => c.toUpper
      }.mkString
    }

    val cachedImage = call.getRow(Schema.CachedImages)
    val origImgUrlRaw = cachedImage.origImgUrl.raw

    sanitizeUrlString(origImgUrlRaw) match {
      case Some(newUrl) =>
        if(urlStringForCompare(newUrl) == urlStringForCompare(origImgUrlRaw)) {
          flowProcess.increment(counterGroup, "Valid Image path", 1)
        }
        else {
          flowProcess.increment(counterGroup, "Illegal Image path (it changed)", 1)
          call.write(cachedImage.md5HashKey.hexstr, origImgUrlRaw, newUrl, cachedImage.origHttpStatusCode, cachedImage.acquireErrMsg, ImageCachingService.cdnUrlStr(cachedImage).getOrElse("Not Cached"))
        }
      case None =>
        flowProcess.increment(counterGroup, "Illegal Image path (URL or URI failed)", 1)
        call.write(cachedImage.md5HashKey.hexstr, origImgUrlRaw, "bad url", cachedImage.origHttpStatusCode, cachedImage.acquireErrMsg, ImageCachingService.cdnUrlStr(cachedImage).getOrElse("Not Cached"))
    }

  })(fldNames: _*)

  grvcascading.flow.connect("IllegalImageURLsJob Main Flow", source, destination, assembly).complete()

  // If so requested, combine the output of the run to a single file.
  if (combineOutput) {
    val outputHdfsFile = outputDirectory + ".tsv"

    try {
      HBaseConfProvider.getConf.fs.delete(new Path(outputHdfsFile), false)
    } catch {
      case ex: Exception =>
    }

    FileUtil.copyMerge(HBaseConfProvider.getConf.fs, new Path(outputDirectory), HBaseConfProvider.getConf.fs,
      new Path(outputHdfsFile), false, HBaseConfProvider.getConf.defaultConf, null)
  }
}
