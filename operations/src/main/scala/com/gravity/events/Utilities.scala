package com.gravity.events

import com.amazonaws.regions.{Region, Regions}
import com.gravity.interests.jobs.hbase.{GrvHBaseConf, HBaseConfProvider}
import org.joda.time.{DateTime, Interval}
import org.apache.hadoop.fs.permission.FsPermission
import com.gravity.utilities.grvstrings._
import com.gravity.utilities.grvtime._
import org.apache.hadoop.fs.{FileSystem, Path => HdfsPath}
import com.gravity.utilities.{HasGravityRoleProperties, Settings}

object Utilities extends HasGravityRoleProperties {
  val basePath = "/mnt/habitat_backups/"

  val hbaseConf: GrvHBaseConf = HBaseConfProvider.getConf
  val fs: FileSystem = HBaseConfProvider.fs

  val hdfsStorageAvroRoot: String = properties.getProperty("recommendation.log.hdfsAvroDir", "/user/gravity/logs.avro/")
  val hdfsWritingAvroRoot: String = properties.getProperty("recommendation.log.hdfsWriteAvroDir", "/user/gravity/logWriting.avro/")
  val hdfsStageSequenceRoot: String = properties.getProperty("recommendation.log.hdfsStagingSequenceDir", "/user/gravity/stageLogs.sequence/")
  val hdfsStageSequenceRootPath = new HdfsPath(hdfsStageSequenceRoot)

  val filePermission = new FsPermission("755")
  val folderPermission = new FsPermission("777")
  val segmentsPerHour = 10
  val minutesPerSegment: Int = 60 / segmentsPerHour

  def hdfsWritingPathFor(year: Int, day: Int, hour: Int, segment: Int, category: String): HdfsPath = {
    val initialPathString = f"$hdfsWritingAvroRoot$category/${year}_$day%03d/hour_$hour%02d_${segment}_"
    var segmentIteration = 0
    def getPath = new org.apache.hadoop.fs.Path(initialPathString + segmentIteration)
    var path = getPath
    while(fs.exists(path)) {
      segmentIteration += 1
      path = getPath
    }
    path
  }

  def hdfsStoragePathFor(year: Int, day: Int, hour: Int, segment: Int, category: String): HdfsPath = {
    val initialPathString = f"$hdfsStorageAvroRoot$category/${year}_$day%03d/hour_$hour%02d_${segment}_"

    var segmentIteration = 0
    def getPath = new org.apache.hadoop.fs.Path(initialPathString + segmentIteration)
    var path = getPath
    while(fs.exists(path)) {
      segmentIteration += 1
      path = getPath
    }
    path
  }

  def s3KeyFor(hdfsPath: HdfsPath): String = {
    val rootSplit = hdfsStorageAvroRoot.toString.split('/') //the part of the path that's hdfs specific
    val fromSplit = hdfsPath.toString.split('/') //the part we will replicate in s3

    val postRootFromSplit = fromSplit.takeRight(fromSplit.length - rootSplit.length)

    postRootFromSplit.mkString("/")
  }

  def getHfdsStageSequenceCategoryDirs: Array[HdfsPath] = fs.listStatus(hdfsStageSequenceRootPath).map(_.getPath)

  def getHdfsCategorySequenceStageDir(category: String): String = s"$hdfsStageSequenceRoot$category/"

  def getCurrentSequenceHdfsStageDir(category: String) : String = {
    val now = new DateTime()
    val hour = now.getHourOfDay
    val minute = now.getMinuteOfHour

    val currentSegment = getSegmentNumber(minute)

    f"${getHdfsCategorySequenceStageDir(category)}${now.toYearDayString}/hour_$hour%02d_$currentSegment/"
  }

  def getSegmentInterval(folderName: String, segmentName: String) : Option[Interval] = {
    //folder formant is [year]_[day]
    //segment format is hour_[0..23]_[0..9]
    val folderParts = folderName.splitBetter("_")
    val segmentParts = segmentName.splitBetter("_")

    for {
      yearString <- folderParts.headOption
      dayString <- folderParts.lift(1)
      year <- yearString.tryToInt
      day <- dayString.tryToInt
      hourString <- segmentParts.lift(1)
      hour <- hourString.tryToInt
      segmentString <- segmentParts.lift(2)
      segment <- segmentString.tryToInt
    } yield {
      val startMinute = math.floor((segment.toFloat / segmentsPerHour.toFloat) * 60F).toInt
      val startTimeMutable = new DateTime(0).toMutableDateTime
      startTimeMutable.setYear(year)
      startTimeMutable.setDayOfYear(day)
      startTimeMutable.setHourOfDay(hour)
      startTimeMutable.setMinuteOfHour(startMinute)
      val startTime = startTimeMutable.toDateTime
      val endTime = startTime.plusMinutes(minutesPerSegment)
      new Interval(startTime, endTime)
    }
  }

  def getSegmentNumber(minute: Int) : Int = {
    math.floor((minute.toFloat / 60F) * segmentsPerHour.toFloat).toInt
  }
}

object s3pathtest extends App {

  import com.amazonaws.auth.BasicAWSCredentials
  import com.amazonaws.services.s3.AmazonS3Client
  import com.amazonaws.services.s3.model.ObjectMetadata
  import com.gravity.interests.jobs.hbase.HBaseConfProvider

  try {
    val awsAccessKey = Settings.getProperty("gravity.eventlogging.key")
    println(awsAccessKey)
    val awsSecretKey = Settings.getProperty("gravity.eventlogging.secret")
    val s3Bucket = Settings.getProperty("gravity.eventlogging.bucket")
    println(s3Bucket)

    //now copy that hdfs file to aws
    val awsCreds = {
      new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    }

    val aws: AmazonS3Client = new AmazonS3Client(awsCreds)
    //aws.setRegion(Region.getRegion(Regions.US_WEST_2))
    println(aws.listObjects(s3Bucket).toString)
  }
  catch {
    case e: Exception => println(e.toString)
  }

}
//
//trait Unifier {
//  import Utilities._
//
//  val sourceCategories : Seq[String]
//  val destCategory : String
//  val year : Int
//  val firstDay : Int
//  val lastDay : Int
//  val removeExisting : Boolean
//  val isClick : Boolean
//
//  RecoServedEvent2.getFields
//  SponsoredStoryServedEvent.getFields
//  ImpressionEvent.getFields
//
//  def unify {
//    val event = ClickEvent.fields
//    val impression = ImpressionEvent.fields
//    var totalLinesWritten = 0L
//    var totalEmptyLines = 0L
//    val days = for { i <- firstDay to lastDay } yield i
//    val hours = for { i <- 0 to 23 } yield i
//
//    days.foreach { day =>
//      println("Processing day " + day)
//      hours.foreach { hour =>
//        val hdfsPath = hdfsPathFor(year, day, hour, destCategory)
//        if(removeExisting && fs.exists(hdfsPath)) {
//          println("removing existing file " + hdfsPath)
//          fs.delete(hdfsPath, false)
//        }
//        if (!fs.exists(hdfsPath)) {
//          println("Processing hour " + hour + ". Writing to " + hdfsPath)
//          val output = new BufferedWriter(new OutputStreamWriter(fs.create(hdfsPath)))
//          var linesWritten = 0L
//          var emptyLines = 0L
//          try {
//            sourceCategories.foreach(category => {
//              val sourcePath = pathFor(year, day, category)
//              println("Reading from source category " + sourcePath)
//              val file = new java.io.File(sourcePath)
//              if (file != null) {
//                val fileList = file.listFiles()
//                if(fileList != null) {
//                  val files = fileList.filter {
//                    file => {
//                      val name = file.getName
//                      val hourString = "hour_" + hour
//                      name.startsWith(hourString) && name.replace(hourString, "").startsWith(".") //i'm sure this could be a regex but nuts to that
//                    }
//                  }.sortBy(f => f.getName)
//                  files.foreach(f => {
//                    val inputPath = f.getAbsolutePath
//                    println("Reading from " + inputPath)
//                    val reader = Source.fromFile(inputPath, Charsets.UTF_8.name())
//                    try {
//                      reader.getLines().foreach(line => {
//                        transformLine(category, line) match {
//                          case Some(transformed) => {
//                            output.write(transformed + "\n")
//                            linesWritten += 1
//                            if (linesWritten % 1000 == 0) println("Written " + linesWritten + " lines")
//                          }
//                          case None => {
//                            emptyLines += 1
//                          }
//                        }
//                      })
//                    }
//                    finally {
//                      reader.close()
//                      output.flush()
//                    }
//                  })
//                }
//                else {
//                  println("No files found for hour " + hour + " in " + sourcePath)
//                }
//              }
//            })
//          }
//          finally {
//            output.close()
//            println("Wrote " + linesWritten + " and had " + emptyLines + " empty lines for " + hdfsPath)
//            totalLinesWritten += linesWritten
//            totalEmptyLines += emptyLines
//          }
//        }
//        else {
//          println("File " + hdfsPath + " already exists")
//        }
//      }
//    }
//
//    println("Wrote " + totalLinesWritten + " lines total and had " + totalEmptyLines + " total empty lines")
//  }
//
//  def transformLine(category: String, line: String): Option[String] = {
//    try {
//      if(category == "magellan") {
//        val magBeacon = BeaconEvent.fromTokenizedString(line, rawBeaconFormat = true)
//        for {
//          rawUrl <- magBeacon.rawUrl
//          grcc2 <- GrccableEvent.grcc2Re.findFirstIn(rawUrl)
//          articleKeys = List(ArticleKey(magBeacon.url.getOrElse(URLUtils.normalizeUrl(rawUrl))))
//          event <- GrccableEvent.fromGrcc2(grcc2, true, articleKeys)
//        } yield {
//          event match {
//            case reco:RecoServedEvent2 => {
//              reco.setClickFields(ClickFields(new DateTime(HDFSLogManager.getBeaconMillisOrEpoch(magBeacon)), magBeacon.url.getOrElse(""), magBeacon.browserUserAgent.getOrElse(""), magBeacon.referrer.getOrElse(""),rawUrl))
//              CampaignService.recoServedToClickEvent(reco).toDelimitedFieldString
//            }
//            case sponsored:SponsoredStoryServedEvent => {
//              sponsored.setClickFields(ClickFields(new DateTime(HDFSLogManager.getBeaconMillisOrEpoch(magBeacon)), magBeacon.url.getOrElse(""), magBeacon.browserUserAgent.getOrElse(""), magBeacon.referrer.getOrElse(""),rawUrl))
//              ClickEvent.from(sponsored).toDelimitedFieldString
//            }
//            case click:ClickEvent => {
//              click.setClickFields(ClickFields(new DateTime(HDFSLogManager.getBeaconMillisOrEpoch(magBeacon)), magBeacon.url.getOrElse(""), magBeacon.browserUserAgent.getOrElse(""), magBeacon.referrer.getOrElse(""),rawUrl))
//              click.toDelimitedFieldString
//            }
//          }
//        }
//      }
//      else {
//        FieldValueRegistry.getInstanceFromString(line) match {
//          case Success(readEvent: FieldWriter) => {
//            if(isClick) {
//              readEvent match {
//                case reco:RecoServedEvent2 => {
//                  Some(CampaignService.recoServedToClickEvent(reco).toDelimitedFieldString)
//                }
//                case sponsored:SponsoredStoryServedEvent => {
//                  Some(ClickEvent.from(sponsored).toDelimitedFieldString)
//                }
//                case click:ClickEvent => {
//                  Some(click.toDelimitedFieldString)
//                }
//              }
//            }
//            else {
//              readEvent match {
//                case recoServed:RecoServedEvent2 => {
//                  Some(CampaignService.from(recoServed).toDelimitedFieldString)
//                }
//                case sponsored:SponsoredStoryServedEvent => {
//                  Some(ImpressionEvent.from(sponsored).toDelimitedFieldString)
//                }
//                case impression:ImpressionEvent => {
//                  Some(impression.toDelimitedFieldString)
//                }
//              }
//            }
//          }
//          case Failure(fails) => {
//            println("Could not read line " + line + ": " + fails)
//            None
//          }
//        }
//      }
//    }
//    catch {
//      case e:Exception => {
//        println("Exception transforming line " + line + ": " + ScalaMagic.formatException(e))
//        None
//      }
//    }
//  }
//
//}
//
//object ClickUnifier extends App with Unifier {
//  val removeExisting = true
//  val sourceCategories = Seq("magellan")
//  val destCategory = "clickEvent-restored"
//  val year = 2013
//  val isClick = true
//  val firstDay = 60
//  val lastDay = 90
//
//  unify
//}
//
//object ImpressionUnifier extends App with Unifier {
//  val isClick = false
//  val removeExisting = false
//  val sourceCategories = Seq("recosdeliveredv2-new", "sponsoredStoryDelivered-new")
//  val destCategory = "impressionServed"
//  val year = 2013
//  val firstDay = 1
//  val lastDay = 42
//
//  unify
//}
//
//object HdfsRedoer extends App {
//  val categoriesToCopy = Seq("impressionServed")
//  val year = 2013
//  val firstDay = 269
//  val lastDay = 269
//  val daysToCopy = for { i <- firstDay to lastDay } yield i
//  val basePath = "/mnt/habitat_backups/"
//  categoriesToCopy.foreach(category => {
//    daysToCopy.foreach(day => {
//      val dayPath =  basePath + category + "/" + year.toString + "_" + day.toString + "/"
//      println("Processing files in " + dayPath)
//      Directory(dayPath).files.foreach {
//        file => {
//          val nameParts = file.name.split(Array('.', '_'))
//          //println("Hour is " + nameParts(1))
//          val hourString = nameParts(1)
//          hourString.tryToInt.map(hour => {
//            if(hour < 17) {
//              println("copying file " + file.path)
//              //HDFSLogManager.copyFileToHDFS(category, file.path, true)
//            }
//          })
////
//        }
//      }
//    })
//  })
//  readLine("enter to finish")
//
//}
//
//object MagRedoer extends App {
//  val categoriesToCopy = Seq("magellan")
//  val daysToCopy = Seq("128")//, "28", "29", "30", "31", "32")
//  val basePath = "/mnt/habitat_backups/"
//  categoriesToCopy.foreach(category => {
//    val categoryPath = basePath + category + "/2013_"
//    daysToCopy.foreach(day => {
//      val dayPath = categoryPath + day + "/"
//      println("Processing files in " + dayPath)
//      Directory(dayPath).files.foreach {
//        file => {
//          println("copying file " + file.path)
//          HDFSLogManager.copyFileToHDFS(category, file.path, true)
//        }
//      }
//    })
//  })
//  readLine("enter to finish")
//
//}
//
//object RedirectClickCopier extends App {
//  val sourcePath = "/mnt/habitat_backups/recosclicked-redirect"
//  val file = new java.io.File(sourcePath)
//  if (file != null) {
//    val fileList = file.listFiles()
//    if(fileList != null) {
//      val files = fileList.filter {
//        file => {
//          file.isDirectory && file.getName.startsWith("2013")
//        }
//      }
//      files.foreach(f => {
//        val dayPath = f.getAbsolutePath
//        println("Processing files in " + dayPath)
//        Directory(dayPath).files.foreach {
//          file => {
//            println("copying file " + file.path)
//            HDFSLogManager.copyFileToHDFS("recosclicked-redirect", file.path)
//          }
//        }
//      })
//    }
//  }
//  else {
//    println(sourcePath + " does not exist, moving on")
//  }
//
//}
//
//object SponsoredStoryServedNewCopier extends App {
//  val sourcePath = "/mnt/habitat_backups/sponsoredStoryClicked-new/2012_323"
//
//  Directory(sourcePath).files.foreach {
//    file => {
//      println("copying file " + file.path)
//      HDFSLogManager.copyFileToHDFS("sponsoredStoryClicked-new", file.path)
//    }
//  }
//}
//written for old log format needs to be updated
//object RecoClickedChecker extends App {
//  val sourcePath = "/user/gravity/logs/recosclicked/2012_222/"
//
//  grvhadoop.perHdfsDirectoryLine(Schema.fs, sourcePath) {
//    line =>
//      HDFSLogManager.recosclickedTransform(line) match {
//        case Some(click) => {
//          println(click)
//        }
//        case None => {
//
//        }
//      }
//  }
//}

//written for the old log format needs to be updatd to use the new one
//object RecoServedFixer extends App {
//
//  val sourcePath = "/mnt/habitat_backups/recosdeliveredv2/2012_319"
//  val destPath = "/user/gravity/logs/recosdeliveredv2/2012_319"
//
//  Directory(sourcePath).files.foreach {
//    file => {
//      val hour = file.name.split('_')(1).split('.')(0).toInt
//      var pre12lines = 0
//      var noonlines = 0
//      var post12lines = 0
//      var traslated12lines = 0
//      var errors = 0
//      println("Hitting file " + file.name)
//      val reader = file.bufferedReader()
//      val outpath = destPath + "/" + file.name
//      println("outputting to " + outpath)
//
//      if(HBaseConfProvider.getConf.fs.exists(new Path(outpath))) {
//        println("File " + outpath + " already exists!")
//      }
//      else {
//        grvhadoop.withHdfsWriter(Schema.fs, destPath + "/" + file.name, true) {
//          writer => {
//            var line = reader.readLine()
//            while (line != null) {
//              val newLine =
//                if(hour < 12) {
//                  RecoServedEvent2.fromTokensNoCurrentUrl(tokenize(line, "^"), line) match {
//                    case Success(obj) => {
//                      val newLine = obj.toDelimitedString
//                      pre12lines += 1
//                      Some(newLine)
//                    }
//                    case Failure(fail) => {
//                      errors += 1
//                      println(fail)
//                      None
//                    }
//                  }
//                }
//                else if(hour > 12) {
//                  post12lines += 1
//                  Some(line)
//                }
//                else {
//                  val tokens = tokenize(line, "^")
//                  if(tokens.size < 20) {
//                    RecoServedEvent2.fromTokensNoCurrentUrl(tokens, line) match {
//                      case Success(obj) => {
//                        val newLine = obj.toDelimitedString
//                        traslated12lines += 1
//                        noonlines += 1
//                        //println(newLine)
//                        Some(newLine)
//                      }
//                      case Failure(fail) => {
//                        println(fail)
//                        errors += 1
//                        None
//                      }
//                    }
//                  }
//                  else {
//                    noonlines += 1
//                    Some(line)
//                  }
//                }
//              newLine match {
//                case Some(stuff) => {
//                  writer.write(stuff + "\n")
//                  //println(stuff)
//                }
//                case None => {}
//              }
//              line = reader.readLine()
//            }
//          }
//        }
//      }
//
//      println("Got " + pre12lines + " before 12 lines, " + noonlines + " noon lines, and " + post12lines + " after noon lines. " + errors + " errors. Translated " + traslated12lines + " lines at noon for " + file.name)
//    }
//  }
//
//}

//written for the old log format, needs to be updated to use the new one
//object SponsoredFixer extends App {
//
//  val sourcePath = "/mnt/habitat_backups/sponsoredStoryClicked/2012_319"
//  val destPath = "/user/gravity/logs/sponsoredStoryClicked/2012_319-movedcurrent"
//
//  Directory(sourcePath).files.foreach {
//    file => {
//
//      var guids = 0
//      var currentUrls = 0
//      var canttell = 0
//      var errors = 0
//      println("Hitting file " + file.name)
//      val reader = file.bufferedReader()
//      val outpath = destPath + "/" + file.name
//      println("outputting to " + outpath)
//
//      if(HBaseConfProvider.getConf.fs.exists(new Path(outpath))) {
//        println("File " + outpath + " already exists!")
//      }
//      else {
//        grvhadoop.withHdfsWriter(Schema.fs, destPath + "/" + file.name, true) {
//          writer => {
//            var line = reader.readLine()
//
//            while (line != null) {
//              val tokens = tokenize(line, "^")
//              val notSure = tokens(tokens.size-1) //could be a currenturl, could be a why
//              val newLine = {
//                if(notSure.contains("|") || notSure == "ccbe008d3b31e330f626bf044c20a11c" || notSure == "5d1b0b06520ce7985a3a2fdb2a8146d5") {
//                  guids += 1
//                  //if it ends in a guids, then it's the newer format and can be echoed
//                  Some(line)
//                }
//                else if(notSure.contains("http://")) {
//                  currentUrls += 1
//                  //if it ends in a current url, it's the older format and has to be redone
//                  SponsoredStoryServedEvent.fromTokensWithCurrent(tokens, line) match {
//                    case Success(n) => Some(n.toDelimitedString)
//                    case Failure(fail) => {
//                      println("reading " + line + " failed: " + fail)
//                      errors += 1
//                      None
//                    }
//                  }
//                }
//                else {
//                  println("don't know what " + notSure + " is")
//                  canttell += 1
//                  None
//                }
//              }
//              newLine match {
//                case Some(stuff) => {
//                  writer.write(stuff + "\n")
//                  //println(stuff)
//                }
//                case None => {}
//              }
//              line = reader.readLine()
//            }
//          }
//        }
//      }
//      println("found " + guids + " guids, " + currentUrls + " currents, and " + canttell + " cant tells, with " + errors + " errors")
//    }
//  }
//
//}

//uses the old event format, needs to be updated to use the new one
//object RecoImpressionMetricsRedoer extends App {
//  val startDay = 198
//  val endDay = 222
//
//  val siteAccumulator = mutable.Map[SiteKey, mutable.Map[ArticleRecommendationMetricKey, Long]]()
//  val articleAccumulator = mutable.Map[ArticleKey, mutable.Map[ArticleRecommendationMetricKey, Long]]()
//
//
//  for (i <- startDay to endDay) {
//    try {
//      redoDay(i)
//    }
//    catch {
//      case e: Exception => {
//        println("Exception processing day " + i + " : " + ScalaMagic.formatException(e))
//      }
//    }
//  }
//
//  def redoDay(day: Int) {
//    def getPathForDay(day: Int) = {
//      "/mnt/habitat_backups/recosdeliveredv2/2012_" + day
//    }
//    val path = getPathForDay(day)
//    var eventsFound = 0
//    var linesFailedToParse = 0
//    println("working from " + path)
//    Directory(path).files.foreach {
//      file => {
//        println("Hitting file " + file.name)
//        val reader = file.bufferedReader()
//        var line = reader.readLine()
//        while (line != null) {
//          RecoServedEvent2.fromDelimitedString(line) match {
//            case Success(event: GrccableEvent) => {
//              eventsFound += 1
//              processEvent(event, isClick = false)
//            }
//            case Failure(fails) => {
//              linesFailedToParse += 1
//              println("Could not parse line " + line + ": " + fails)
//            }
//            case Success(wrongType) => {
//              linesFailedToParse += 1
//              println("Could not parse line " + line + ": " + wrongType)
//            }
//          }
//          line = reader.readLine()
//        }
//      }
//    }
//    println("Got " + eventsFound + " and had " + linesFailedToParse + " fail to parse")
//    flushAndStoreArticles()
//    flushAndStoreSites()
//
//  }
//
//  def flushSites: Map[SiteKey, Map[ArticleRecommendationMetricKey, Long]] = {
//    for ((k, v) <- siteAccumulator) yield {
//      siteAccumulator.remove(k)
//      k -> v
//    }
//  }
//
//  def flushArticles: Map[ArticleKey, Map[ArticleRecommendationMetricKey, Long]] = {
//    for ((k, v) <- articleAccumulator) yield {
//      articleAccumulator.remove(k)
//      k -> v
//    }
//  }
//
//  def flushAndStoreSites() {
//    val sites = flushSites
//    println("Flushing " + sites.size + " sites...")
//    for ((sk, recoMetricMap) <- sites) {
//      try {
//        Schema.Sites.put(sk).valueMap(_.recommendationMetrics, recoMetricMap).execute()
//      }
//      catch {
//        case ex: Exception => {
//          println("Failed to flush live recommendation metrics for site: " + sk.siteId + ": " + ScalaMagic.formatException(ex))
//        }
//      }
//    }
//  }
//
//  def flushAndStoreArticles() {
//    val articles = flushArticles
//    println("Flushing " + articles.size + " articles...")
//    for ((ak, recoMetricMap) <- articles) {
//      try {
//        Schema.Articles.put(ak).valueMap(_.recommendationMetrics, recoMetricMap).execute()
//      }
//      catch {
//        case ex: Exception => {
//          println("Failed to flush live recommendation metrics for article: " + ak.articleId + ": " + ScalaMagic.formatException(ex))
//        }
//      }
//    }
//  }
//
//
//  def processEvent(event: GrccableEvent, isClick: Boolean) {
//
//    val siteGuid = event.getSiteGuid
//
//    if (SiteService.supportsLiveRecommendationMetrics(siteGuid)) {
//      if (isClick || event.isInstanceOf[RecoServedEvent2]) {
//        val articleIds = event.getArticleIds
//        if (articleIds.isEmpty) {
//
//        } else {
//
//          val longOne = 1l
//          val recoMetricsMap = if (isClick) {
//            Map(ArticleRecommendationMetricKey.byClick(event) -> longOne)
//          } else {
//            val byArticleImpression = ArticleRecommendationMetricKey.byArticleImpression(event)
//            val byUnitImpression = byArticleImpression.byUnitImpression
//            Map(byArticleImpression -> longOne, byUnitImpression -> longOne)
//          }
//
//          val articleKeys = articleIds.map(aid => ArticleKey(aid)).toSet
//
//          for (ak <- articleKeys) {
//            accumulate(ak, recoMetricsMap)
//          }
//
//          val numArticles = articleKeys.size.toLong
//
//          val siteRecoMetMap = recoMetricsMap.map(kv => {
//            if (kv._1.countBy == RecommendationMetricCountBy.articleImpression) {
//              kv._1 -> numArticles
//            } else {
//              kv
//            }
//          })
//
//          accumulate(SiteKey(siteGuid), siteRecoMetMap)
//        }
//      }
//      else {
//        if (!isClick) {
//          //then it's a sponsored story impression, which currently would be double counted
//
//        }
//      }
//    }
//  }
//
//
//  def accumulate[K](key: K, keyVals: Map[ArticleRecommendationMetricKey, Long]) {
//    val sourceMap = key match {
//      case sk: SiteKey => siteAccumulator.getOrElseUpdate(sk, mutable.Map[ArticleRecommendationMetricKey, Long]())
//      case ak: ArticleKey => articleAccumulator.getOrElseUpdate(ak, mutable.Map[ArticleRecommendationMetricKey, Long]())
//      case wtf => return
//    }
//
//    for ((k, v) <- keyVals) {
//      val previousValue = sourceMap.getOrElse(k, 0l)
//      sourceMap.update(k, previousValue + v)
//    }
//  }
//
//
//}



/**
 * This is a lil script that will, given a day's worth of beacons, recreate the recos clicked logs.
 */
//this uses the old log format, needs to be updated to use the new
//object RecoClickedRedoer extends App {
//  val i = 319
//  for (j <- 319 to 319) {
//
//    try {
//
//      val sourcePath = "/mnt/habitat_backups/magellan/2012_" + i
//      val destPath = "/user/gravity/logs/recosclicked/2012_" + i
//
//      println("Working with directory " + sourcePath)
//      println("Working with dest " + destPath)
//      var clicks = 0
//      var notClicks = 1
//      var lines = 0
//
//      Directory(sourcePath).files.foreach {
//        file =>
//          println("Hitting file " + file.name)
//          val reader = file.bufferedReader()
//          grvhadoop.withHdfsWriter(Schema.fs, destPath + "/" + file.name, true) {
//            writer =>
//              var line = reader.readLine()
//              while (line != null) {
//                HDFSLogManager.recosclickedTransform(line) match {
//                  case Some(clicked) => {
//                    writer.write(clicked + "\n")
//                    clicks += 1
//                  }
//                  case None => {
//                    notClicks += 1
//                  }
//                }
//
//                lines += 1
//                if (lines % 10000 == 0) println("Line: " + lines + " Clicks: " + clicks + " Not Clicks: " + notClicks)
//
//                line = reader.readLine()
//              }
//          }
//      }
//
//    }
//    catch {
//      case ex: Exception => {
//        println("Died while working on day " + i)
//      }
//    }
//
//  }
//}
