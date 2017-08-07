package com.gravity.interests.jobs.intelligence.schemas

import java.io.InputStream

import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest


/**
 * Created by agrealish14 on 2/12/15.
 */
class RecommendationS3Test extends BaseScalaTest with operationsTesting {

  ignore("test upload json") {

    RecommendationsS3.upsertKeyValue("test-upload", "{  \"version\" : 3}")

  }

  ignore("test file upload") {

    val is: InputStream = getClass.getResourceAsStream("testFallbackRecommendation.json")
    val fileAsString = scala.io.Source.fromInputStream(is).mkString
    RecommendationsS3.upsertKeyValue("test-upload-file", fileAsString)
  }


  ignore("test file key adjustment") {

    val is: InputStream = getClass.getResourceAsStream("testFallbackRecommendation.json")
    val fileAsString = scala.io.Source.fromInputStream(is).mkString
    RecommendationsS3.upsertKeyValue("v5 - 95ec266b244de718b80c652a08af06fa - dev:tablet - geo:DJ - reco:81 - sp:3924 - buck:2 - sec:default", fileAsString)
  }

  ignore("test fetch") {

    val is: InputStream = getClass.getResourceAsStream("testFallbackRecommendation.json")
    val fileAsString = scala.io.Source.fromInputStream(is).mkString

    val downloadedFileAsString = RecommendationsS3.get("v5 - 95ec266b244de718b80c652a08af06fa - dev:tablet - geo:DJ - reco:81 - sp:3924 - buck:2 - sec:default")
    //println("'" + downloadedFileAsString + "'")
    //println("'" + fileAsString + "'")
    //assert(fileAsString.equals(downloadedFileAsString))

    //println(RecommendationsS3.get("defaultrecos-v7 - sg:2d025da6b8ac5a2af0f767865bc73512 - sp:4992 - buck:1 - slot:0 - geo:US - dev:mobile - reco:282 - sec:default"))
    //println(RecommendationsS3.get("v7 - sg:2d025da6b8ac5a2af0f767865bc73512 - sp:4992 - buck:1 - slot:0 - geo:US - dev:mobile - reco:282 - sec:default"))

    //val downloadedFileAsString = RecommendationsS3.get("v7 - sg:aaae90a45699535d9826a07b0d15039b - sp:3266 - buck:15 - slot:1 - geo:intl - dev:mobile - reco:257 - sec:default")
    //println("'" + downloadedFileAsString + "'")

  }

/*
  ignore("test deserialize") {

    val is: InputStream = getClass.getResourceAsStream("testFallbackRecommendation.json")
    val fileAsString = scala.io.Source.fromInputStream(is).mkString

    val downloadedFileAsString = RecommendationsS3.get("v5 - 95ec266b244de718b80c652a08af06fa - dev:tablet - geo:DJ - reco:81 - sp:3924 - buck:2 - sec:default")

    //println("'" + downloadedFileAsString + "'")
    //println("'" + fileAsString + "'")
    //assert(fileAsString.equals(downloadedFileAsString))

    //val defaultRecos = Json.fromJson[RecoResult](Json.parse(downloadedFileAsString))(RecoResult.jsonFormat).asOpt
  }
*/

}

