package com.gravity.test

import com.gravity.interests.jobs.intelligence.operations.{ImageCachingService, CachedImageRowVals}
import com.gravity.interests.jobs.intelligence._
import com.gravity.interests.jobs.intelligence.SchemaTypes._
import com.gravity.utilities.{HashUtils, BaseScalaTest}
import com.gravity.valueclasses.ValueClassesForDomain._

import scalaz._, Scalaz._

class ImageCachingServiceTest extends BaseScalaTest with operationsTesting with SerializationTesting {
  val strs  = (1 to 4).map(i => "str" + i)
  val longs = List(1L << 40, 1L << 39)
  val ints  = List(1 << 30, 1 << 29, 1 << 28)

  def allOptions[T](i: T) = List(Option(i), None)

  //
  // Test ComplexByteConverter deep-clone serialization of data types used by CachedImagesTable
  //

  test("test ScopedKey Serialization") {
    val artKey     = ArticleKey("http://sample.com/path/index.html")
    val siteKey    = SiteKey("FAKEY")
    val campKey    = CampaignKey(siteKey, 123L)
    val campArtKey = CampaignArticleKey(campKey, artKey)

    val scopedKeys = List(artKey, siteKey, campKey, campArtKey).map(_.toScopedKey)

    info(s"Testing Round-Trip of ${scopedKeys.size} ScopedKeys")

    scopedKeys.foreach { scopedKey =>
      assertResult(scopedKey, "sScopedKey round-trip should create a faithful deep clone of $scopedKey.") {
        deepCloneWithComplexByteConverter(scopedKey)
      }
    }
  }

  test("Test MD5HashKey Serialization") {
    val md5HashKeys = strs.map(str => MD5HashKey(HashUtils.md5(str)))

    info(s"Testing MD5HashKeyConverter Round-Trip of ${md5HashKeys.size} MD5HashKeys")

    // Test serialization of the MD5HashKey class
    md5HashKeys.foreach { md5HashKey =>
      assertResult(md5HashKey, "MD5HashKeyConverter round-trip should create a faithful deep clone of a MD5HashKey.") {
        deepCloneWithComplexByteConverter(md5HashKey)
      }
    }
  }

  test("Test ImageUrl Serialization") {
    val imageUrls = strs map ImageUrl

    info(s"Testing ImageUrlConverter Round-Trip of ${imageUrls.size} ImageUrls")

    // Test serialization of the ImageUrl class
    imageUrls.foreach { imageUrl =>
      assertResult(imageUrl, "ImageUrlConverter round-trip should create a faithful deep clone of a ImageUrl.") {
        deepCloneWithComplexByteConverter(imageUrl)
      }
    }
  }

  test("Test Option[Long] Serialization") {
    val optLongs = for {
      long <- longs
      optLong <- allOptions(long)
    } yield optLong

    info(s"Testing OptionLongConverter Round-Trip of ${optLongs.size} Option[Long]s")

    // Test serialization of an Option[Long]
    optLongs.foreach { optLong =>
      assertResult(optLong, "OptionLongConverter round-trip should create a faithful deep clone of a Option[Long].") {
        deepCloneWithComplexByteConverter(optLong)
      }
    }
  }

  test("Test ImageShapeAndSizeKey Serialization") {
    val imageShapeAndSizeKeys = strs map ImageShapeAndSizeKey

    info(s"Testing ImageShapeAndSizeKeyConverter Round-Trip of ${imageShapeAndSizeKeys.size} ImageShapeAndSizeKeys")

    // Test serialization of the ImageShapeAndSizeKey class
    imageShapeAndSizeKeys.foreach { imageShapeAndSizeKey =>
      assertResult(imageShapeAndSizeKey, "ImageShapeAndSizeKeyConverter round-trip should create a faithful deep clone of a ImageShapeAndSizeKey.") {
        deepCloneWithComplexByteConverter(imageShapeAndSizeKey)
      }
    }
  }

  test("Test CachedImageInfo Serialization") {
    val cachedImageInfos = for {
      str1 <- strs
      int1 <- ints
      dstScheme1 = ImageCacheDestScheme(str1, int1)

      str2 <- strs
      str3 <- strs
      long4 <- longs

      int5 <- ints
      optInt5 <- allOptions(int5)

      int6 <- ints
      optInt6 <- allOptions(int6)
    } yield {
      CachedImageInfo(dstScheme1, str2, str3, long4, optInt5, optInt6)
    }

    info(s"Testing CachedImageInfoConverter Round-Trip of ${cachedImageInfos.size} CachedImageInfos")

    // Test serialization of the CachedImageInfo class
    cachedImageInfos.foreach { cachedImageInfo =>
      assertResult(cachedImageInfo, "CachedImageInfoConverter round-trip should create a faithful deep clone of a CachedImageInfo.") {
        deepCloneWithComplexByteConverter(cachedImageInfo)
      }
    }
  }

  //
  // Test the safe extraction of human-readable image URLs from our cached image URLs.
  //

  val validCached = "http://interestimages.grvcdn.com/img/ssl.gstatic.com/9fca347d1f32d84bdde2b5dfb4b1b2bf-orig.png?use=true&status=200&srcUrl=https%3A%2F%2Fssl.gstatic.com%2Fgb%2Fimages%2Fcst.png%3F01"
  val validHuman  = "https://ssl.gstatic.com/gb/images/cst.png?01"
  val tweakedMd5  = "http://interestimages.grvcdn.com/img/ssl.gstatic.com/9fca347d1f32d84bdde2b5dfb4b1b2be-orig.png?use=true&status=200&srcUrl=https%3A%2F%2Fssl.gstatic.com%2Fgb%2Fimages%2Fcst.png%3F01"
  val tweakedOrig = "http://interestimages.grvcdn.com/img/ssl.gstatic.com/9fca347d1f32d84bdde2b5dfb4b1b2bf-orig.png?use=true&status=200&srcUrl=https%3A%2F%2Fssl.gstatic.com%2Fgb%2Fimages%2Fcst.png%3F02"

  test("asHumanOptImgStr should handle optImg None correctly") {
    assertResult(None) {
      ImageCachingService.asHumanOptImgStr(None)
    }
  }

  test("asOptHumanOptCdnPair should handle optImg None correctly") {
    assertResult(true) {
      List(false, true).forall { wantCdn =>
        ImageCachingService.asOptHumanOptCdnPair(wantCdn, None) == (None, None)
      }
    }
  }

  test("A valid cached image URL should be correctly parseable as a human image URL") {
    assertResult(true) {
      ImageCachingService.asHumanImgStr(validCached) == validHuman &&
      ImageCachingService.asHumanOptImgStr(validCached.some) == validHuman.some
    }
  }

  test("An empty string should be returned unchanged by the 'as human' methods") {
    assertResult(true) {
      ImageCachingService.asHumanImgStr("") == "" &&
      ImageCachingService.asHumanOptImgStr("".some) == "".some
    }
  }

  test("An original human URL should be returned unchanged by the 'as human' methods") {
    assertResult(true) {
      ImageCachingService.asHumanImgStr(validHuman) == validHuman &&
      ImageCachingService.asHumanOptImgStr(validHuman.some) == validHuman.some
    }
  }

  test("A tweaked cachedImageUrl should fail to parse as a well-known human image URL") {
    val tweaks = List(tweakedMd5, tweakedOrig)

    assertResult(true) {
      tweaks.forall { tweak =>
        ImageCachingService.asHumanImgStr(tweak) == tweak &&
        ImageCachingService.asHumanOptImgStr(tweak.some) == tweak.some
      }
    }
  }

  test("asOptHumanOptCdnPair should produce expected outputs") {
    assertResult(true) {
      val cases = List(
        (true , None            , (None, None)),
        (false, None            , (None, None)),
        (true , validCached.some, (validHuman.some , validCached.some)),
        (true , validHuman.some , (validHuman.some , None)),
        (false, validCached.some, (validHuman.some , None)),
        (false, validHuman.some , (validHuman.some , None)),
        (true , tweakedMd5.some , (tweakedMd5.some , None)),
        (false, tweakedMd5.some , (tweakedMd5.some , None)),
        (true , tweakedOrig.some, (tweakedOrig.some, None)),
        (false, tweakedOrig.some, (tweakedOrig.some, None))
      )

      cases.forall { testCase =>
        ImageCachingService.asOptHumanOptCdnPair(testCase._1, testCase._2) == testCase._3
      }
    }
  }

  //
  // Test the database I/O of CachedImagesTable.
  //

  test("Test I/O to the whereUsed family") {
    val ak = ArticleKey("http://www.sample.com/interesting/article.html")
    val sg = "FAKE_SITE_GUID"
    val sk = SiteKey(sg)
    val ck = CampaignKey(sk, -123456789L)
    val ca = CampaignArticleKey(ck, ak)

    def buildImgUrl(idx: Int) = ImageUrl(s"http://www.sample.com/intersting/image${idx}.jpg")

    val idxRange = 0 until 4

    val idxToWriteKeys = (for (idx <- idxRange) yield {
      val akList = if ((idx & 1) != 0) List(ak) else Nil
      val caList = if ((idx & 2) != 0) List(ca) else Nil

      idx -> (akList ::: caList).map(_.toScopedKey).toSet
    }).toMap

    idxRange.foreach { idx =>
      ImageCachingService.addWhereUsed(buildImgUrl(idx), idxToWriteKeys(idx))
    }

    idxRange.foreach { idx =>
      val wantKeys = idxToWriteKeys(idx)
      val readKeys = ImageCachingService.readCachedImageRow(buildImgUrl(idx), true).toOption.map(_.whereUsed.keySet).getOrElse(Set())

      assertResult(wantKeys, "Expected readKeys == wantKeys") {
        readKeys
      }
    }

    idxRange.foreach { idx =>
      ImageCachingService.deleteWhereUsed(buildImgUrl(idx), Set(ak.toScopedKey))
    }

    idxRange.foreach { idx =>
      val wantKeys = idxToWriteKeys(idx).filter(_ != ak.toScopedKey)
      val readKeys = ImageCachingService.readCachedImageRow(buildImgUrl(idx), true).toOption.map(_.whereUsed.keySet).getOrElse(Set())

      assertResult(wantKeys, s"After Zap1, idx=$idx, expected readKeys == wantKeys") {
        readKeys
      }
    }
  }

  test("Test ImageCachingService.saveAcquireResults vs. ImageCachingService.readCachedImageRow") {
    // Generate a bunch of fake CachedImageRow values.
    val seed = System.currentTimeMillis
    val srnd = new scala.util.Random(seed)

    val origCachedImageVals = (for {
      count <- 1 to 1000
    } yield CachedImageRowVals.nextCachedImageRowVals(srnd)).toSet

    println(s"Attempting to round-trip ${origCachedImageVals.size} CachedImageRowVals...")

    // Save the fake CachedImageRow values.
    origCachedImageVals.foreach { imgVal =>
      val result = ImageCachingService.saveAcquireResults(
        imgVal.origImgUrl,
        imgVal.lastAcquireTryTime,
        imgVal.lastAcquireTryCount,
        imgVal.origHttpStatusCode,
        imgVal.origContentType,
        imgVal.origContentLength,
        imgVal.acquiredOk,
        imgVal.acquireErrPhase,
        imgVal.acquireErrMsg,
        imgVal.cachedVersions)

      assertResult(true, "Expected successful read from HBase") {
        result.isSuccess
      }
    }

    // This is what we expect to read back from the database.
    val origOptSet : Set[Option[CachedImageRowVals]] = origCachedImageVals.map(_.some)

    // See if we get the desired results when reading back using the origImgUrl as the read key.
    assertResult(origOptSet, "ImageCachingService saveAcquireResults/readCachedImageRow round-trip should return same values as written.") {
      val readOptSet = origCachedImageVals map { origImgValue =>
        ImageCachingService.readCachedImageRow(origImgValue.origImgUrl).toOption map CachedImageRowVals.fromCachedImageRow
      }

      readOptSet
    }

    // See if we get the desired results when reading back using the md5HashKey as the read key.
    val readOptSet = origCachedImageVals map { origImgValue =>
      ImageCachingService.readCachedImageRow(origImgValue.origImgUrl, true).toOption map CachedImageRowVals.fromCachedImageRow
    }

    assertResult(origOptSet, "ImageCachingService saveAcquireResults/readCachedImageRow round-trip should return same values as written.") {
      readOptSet
    }
  }
}

