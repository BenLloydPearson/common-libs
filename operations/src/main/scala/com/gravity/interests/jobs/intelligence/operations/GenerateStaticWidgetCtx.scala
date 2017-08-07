package com.gravity.interests.jobs.intelligence.operations

import com.gravity.service.grvroles
import com.gravity.utilities.Settings2
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime.ONE_MINUTE_SECONDS
import com.gravity.utilities.time.DateHour
import com.gravity.valueclasses.ValueClassesForDomain.SitePlacementId
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.matching.Regex

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

/**
 * @param description Human description of context for logs, etc.
 * @param w2BaseUrl No trailing slash.
 * @param w2TimeoutSecs # of seconds to wait on /w2 before failing generation process.
 * @param apiBaseUrl No trailing slash.
 * @param apiTimeoutSecs # of seconds to wait on reco API before failing generation process.
 * @param jsonpFileForWidget Destination JSONP file name to upload to on S3. If recogen is in failover, the file name
 *                           will be different; see [[jsonpFile()]].
 * @param gcProbability Number between 0-1 indicating chance that a GC cycle should run after uploading a static widget.
 */
case class GenerateStaticWidgetCtx(description: String, w2BaseUrl: String, w2TimeoutSecs: Int, apiBaseUrl: String,
                                   apiTimeoutSecs: Int, private[operations] val jsonpFileForWidget: (StaticWidget) => String,
                                   private val gcProbability: Double, private[operations] val gcHours: Int) {
  /**
   * @param generationTime If provided, the file name will be suffixed with a str representing generationTime intended
   *                       for versioning.
    *
   */
  def jsonpFile(staticWidget: StaticWidget, generationTime: Option[DateTime] = None,
                shouldUseRecogenFailoverPrefix: Boolean = grvroles.shouldUseRecogenFailoverPrefix): String = {
    val baseJsonpFile = {
      val basePath = jsonpFileForWidget(staticWidget)

      if(shouldUseRecogenFailoverPrefix)
        basePath + GenerateStaticWidgetCtx.fromRecogenFailoverSuffix
      else
        basePath
    }

    generationTime.fold(baseJsonpFile)(baseJsonpFile + "." + _.toString(GenerateStaticWidgetCtx.versionedFileNameStr))
  }


  /** @return TRUE if the given S3 object key (file path) indicates the file was made for the given static widget. */
  def isFileForWidget(staticWidget: StaticWidget, s3ObjectKey: String): Boolean = s3ObjectKey startsWith jsonpFileForWidget(staticWidget)

  /**
   * @return If the given S3 object key (file path) is a static widget backup file .
   */
  def backupTimeFromS3FilePath(s3ObjectKey: String): Option[DateTime] = {
    val potentialDateTimePart = s3ObjectKey.takeRight(GenerateStaticWidgetCtx.versionFormatStr.length)

    try {
      Some(GenerateStaticWidgetCtx.versionedFileNameStr.parseDateTime(potentialDateTimePart))
    }
    catch {
      case _: Exception =>
        None
    }
  }

  private[operations] def shouldGarbageCollectBackups: Boolean = Math.random() < gcProbability
}

object GenerateStaticWidgetCtx {
  private val showDevWidget = Settings2.getBooleanOrDefault("recommendation.widget.showDevWidget", false)
  private val versionFormatStr = "YYYY-MM-dd.HH"
  private val versionedFileNameStr = DateTimeFormat.forPattern(versionFormatStr)
  private val versionedFileNamePartRegexStr = """\.\d{4}-\d{2}-\d{2}\.\d{2}"""
  private[operations] val fromRecogenFailoverSuffix = ".fromRecogenFailover"
  private val fromRecogenFailoverSuffixRegexStr = "\\.fromRecogenFailover"

  val devCtx: GenerateStaticWidgetCtx = new GenerateStaticWidgetCtx(
    "DEV",
    "http://localhost:8080/recommendation/w2",
    5 * ONE_MINUTE_SECONDS,
    "http://localhost:8080/public/apiv2/recommendations",
    5 * ONE_MINUTE_SECONDS,
    _.devDestFile,
    1.0,
    gcHours = 72
  )

  val wqaCtx: GenerateStaticWidgetCtx = new GenerateStaticWidgetCtx(
    "WQA",
    "http://wqa.gravity.com/recommendation/w2",
    ONE_MINUTE_SECONDS,
    "http://wqa.gravity.com/public/apiv2/recommendations",
    ONE_MINUTE_SECONDS,
    _.wqaDestFile,
    0.1,
    gcHours = 72
  )

  val prodCtx: GenerateStaticWidgetCtx = new GenerateStaticWidgetCtx(
    "PROD",
    "http://staticwidgets.prod.grv/recommendation/w2",
    10,
    "http://staticwidgets.prod.grv/public/apiv2/recommendations",
    10,
    _.destFile,
    0.1,
    gcHours = 72
  )

  val defaultCtx: GenerateStaticWidgetCtx = if(showDevWidget) devCtx else prodCtx

  /**
   * @return The Match contains groups as defined by the private Regexes in this object.
   *
   * @see keyMatchToGenerationDate
   * @see keyMatchToStaticWidget
   */
  def staticWidgetKeyMatch(s3ObjectKey: String): Option[Regex.Match] = {
    val objectKeySansPathPrefix = s3ObjectKey.takeRightUntil(_ == '/')

    // API widget
    staticApiWidgetObjectKeyMatcher.findFirstMatchIn(objectKeySansPathPrefix)

      // API JSONP widget
      .orElse(staticApiJsonpWidgetObjectKeyMatcher.findFirstMatchIn(objectKeySansPathPrefix))

      // Widget with front end
      .orElse(staticWidgetWithFrontEndObjectKeyMatcher.findFirstMatchIn(objectKeySansPathPrefix))
  }

  def keyMatchToStaticWidget(m: Regex.Match): Option[StaticWidget] = {
    StaticWidget.find(SitePlacementId(m.group("spId").toLong))
  }

  def keyMatchToGenerationDate(m: Regex.Match): Option[DateTime] = try {
    Option(m.group("versionSuffix")).flatMap(_.trimLeft('.').noneForEmpty).map(versionedFileNameStr.parseDateTime)
  }
  catch {
    case ex: NoSuchElementException => None
  }

  /**
   * @return Suffix to append to static widget files on S3 to obtain a versioned file name for that widget for the
   *         given hour.
   */
  def s3FilePathVersionSuffix(dateHour: DateHour): String = {
    '.' +: dateHour.toString(GenerateStaticWidgetCtx.versionedFileNameStr)
  }

  private lazy val staticWidgetWithFrontEndObjectKeyMatcher =
    s"^(devS|wqaS|s)taticWidget(\\d+)\\.js($fromRecogenFailoverSuffixRegexStr)?($versionedFileNamePartRegexStr)?$$"
      .r(emptyString, "spId", "recogenFailoverSuffix", "versionSuffix")

  private lazy val staticApiWidgetObjectKeyMatcher =
    s"^(devS|wqaS|s)taticApiWidget(\\d+)\\.json($fromRecogenFailoverSuffixRegexStr)?($versionedFileNamePartRegexStr)?$$"
      .r(emptyString, "spId", "recogenFailoverSuffix", "versionSuffix")

  private lazy val staticApiJsonpWidgetObjectKeyMatcher =
    s"^(devS|wqaS|s)taticApiWidget(\\d+)\\.js($fromRecogenFailoverSuffixRegexStr)?($versionedFileNamePartRegexStr)?$$"
      .r(emptyString, "spId", "recogenFailoverSuffix", "versionSuffix")
}