package com.gravity.test

import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.operations.ImageBackupService
import com.gravity.utilities.BaseScalaTest

import scalaz._, Scalaz._

class ImageBackupServiceTest extends BaseScalaTest with operationsTesting with SerializationTesting {
  val strs  = (1 to 4).map(i => "str" + i)
  val longs = List(1L << 40, 1L << 39)
  val ints  = List(1 << 30, 1 << 29, 1 << 28)

  def allOptions[T](i: T) = List(Option(i), None)

  //
  // Test ComplexByteConverter deep-clone serialization of data types used by ImageBackupsTable
  //

  test("test ScopedKey Serialization") {
    val artKey     = ArticleKey("http://sample.com/path/index.html")
    val siteKey    = SiteKey("FAKEY")
    val campKey    = CampaignKey(siteKey, 123L)

    val scopedKeys = List(artKey, siteKey, campKey).map(_.toScopedKey)

    info(s"Testing Round-Trip of ${scopedKeys.size} ScopedKeys")

    scopedKeys.foreach { scopedKey =>
      assertResult(scopedKey, "sScopedKey round-trip should create a faithful deep clone of $scopedKey.") {
        deepCloneWithComplexByteConverter(scopedKey)
      }
    }
  }

  //
  // Test the database I/O of ImageBackupsTable.
  //

  case class ImageBackupRowVals(arKey: ArticleKey, arImg: String, casImgs: Map[CampaignKey, Option[String]])

  object ImageBackupRowVals {
    def fromImageBackupRow(row: ImageBackupRow) =
      ImageBackupRowVals(row.articleKey, row.artImage, row.casImages.toMap)

    val seed = System.currentTimeMillis
    val srnd = new scala.util.Random(seed)

    def nextString = srnd.nextInt(2) match {
      case 0 => ""
      case 1 => srnd.nextString(100)
    }

    def nextOptString = srnd.nextInt(2) match {
      case 0 => None
      case 1 => nextString.some
    }

    def nextImageBackupRowVals: ImageBackupRowVals = {
      val arKey = ArticleKey(srnd.nextLong)
      val arImg = nextString

      val casTups: Seq[(CampaignKey, Option[String])] = for (i <- 1 to srnd.nextInt(10)) yield {
        val cmKey = CampaignKey(SiteKey(srnd.nextLong), srnd.nextLong)

        cmKey -> nextOptString
      }

      ImageBackupRowVals(arKey, arImg, casTups.toMap)
    }
  }

  test("Test ImageBackupService.saveRow vs. ImageBackupService.readRow") {
    // Generate a bunch of fake ImageBackupRow values.
    val origImageBackupVals = (for {
      count <- 1 to 1000
    } yield ImageBackupRowVals.nextImageBackupRowVals).toSet

    println(s"Attempting to round-trip ${origImageBackupVals.size} ImageBackupRowVals...")

    // Save the fake ImageBackupRow values.
    origImageBackupVals.foreach { imgVal =>
      val result = ImageBackupService.saveRow(imgVal.arKey, imgVal.arImg, imgVal.casImgs)

      assertResult(true, "Expected successful write to HBase") {
        result.isSuccess
      }
    }

    // This is what we expect to read back from the database.
    val origOptSet : Set[Option[ImageBackupRowVals]] = origImageBackupVals.map(_.some)

    // See if we get the desired results when reading back using the origImgUrl as the read key.
    assertResult(true, "ImageBackupService saveRow/readRow round-trip should return same values as written.") {
      val readOptSet = origImageBackupVals map { origImgValue =>
        ImageBackupService.readRow(origImgValue.arKey, true).toOption map ImageBackupRowVals.fromImageBackupRow
      }

      readOptSet == origOptSet
    }
  }
}


