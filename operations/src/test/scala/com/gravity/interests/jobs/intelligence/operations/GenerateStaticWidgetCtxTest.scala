package com.gravity.interests.jobs.intelligence.operations

import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import com.gravity.utilities.grvstrings._
import com.gravity.valueclasses.ValueClassesForDomain.{BucketId, SitePlacementId}
import org.joda.time.DateTime

/*
              ___...---''
  ___...---'\'___
''       _.-''  _`'.______\\.
        /_.)  )..-  __..--'\\
       (    __..--''
        '-''\@


 Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ Ⓐ
*/

class GenerateStaticWidgetCtxTest extends BaseScalaTest with operationsTesting {
  val DummyStaticWidget = new StaticApiWidget(SitePlacementId(-1L), emptyString) {
    override val devDestFile: String = "devDummyStaticWidget.json"
    override val wqaDestFile: String = "wqaDummyStaticWidget.json"
    override val destFile: String = "dummyStaticWidget.json"
  }

  val DummyStaticWidgetWithForcedBucket = new StaticApiWidget(SitePlacementId(-1L), emptyString, forcedBucket = Some(BucketId(69))) {
    override val devDestFile: String = "devDummyStaticWidget.json"
    override val wqaDestFile: String = "wqaDummyStaticWidget.json"
    override val destFile: String = "dummyStaticWidget.json"
  }

  test("isFileForWidget, backupTimeFromS3FilePath") {
    implicit val staticWidgetCtx = GenerateStaticWidgetCtx.devCtx

    val liveStaticWidgetFile = staticWidgetCtx.jsonpFile(DummyStaticWidget)
    val recogenFailoverLiveStaticWidgetFile = liveStaticWidgetFile + GenerateStaticWidgetCtx.fromRecogenFailoverSuffix
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, liveStaticWidgetFile) should be(true)
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, recogenFailoverLiveStaticWidgetFile) should be(true)
    staticWidgetCtx.backupTimeFromS3FilePath(liveStaticWidgetFile) should be(None)
    staticWidgetCtx.backupTimeFromS3FilePath(recogenFailoverLiveStaticWidgetFile) should be(None)

    val dt = new DateTime().withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0)
    val someDt = Some(dt)
    val backupStaticWidgetFile = staticWidgetCtx.jsonpFile(DummyStaticWidget, someDt)
    val recogenFailoverBackupStaticWidgetFile = recogenFailoverLiveStaticWidgetFile + backupStaticWidgetFile.substring(liveStaticWidgetFile.length)
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, backupStaticWidgetFile) should be(true)
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, recogenFailoverBackupStaticWidgetFile) should be(true)
    staticWidgetCtx.backupTimeFromS3FilePath(backupStaticWidgetFile) should be(someDt)
    staticWidgetCtx.backupTimeFromS3FilePath(recogenFailoverBackupStaticWidgetFile) should be(someDt)

    // Edge cases
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, emptyString) should be(false)
    staticWidgetCtx.isFileForWidget(DummyStaticWidget, "foo") should be(false)
    staticWidgetCtx.backupTimeFromS3FilePath(emptyString) should be(None)
    staticWidgetCtx.backupTimeFromS3FilePath("foo") should be(None)
  }

  test("forced bucket id") {
    implicit val staticWidgetCtx = GenerateStaticWidgetCtx.devCtx

    val apiUrl = DummyStaticWidget.contentUrl

    info("API URL withOUT forced bucket: {0}", apiUrl)

    withClue("contentUrl should NOT contain 'bucket='") {
      apiUrl.contains("bucket=") should be (false)
    }

    val apiUrlWithBucket = DummyStaticWidgetWithForcedBucket.contentUrl

    info("API URL with forced bucket: {0}", apiUrlWithBucket)

    withClue("contentUrl should contain 'bucket=69'") {
      apiUrlWithBucket.contains("bucket=69") should be (true)
    }
  }
}
