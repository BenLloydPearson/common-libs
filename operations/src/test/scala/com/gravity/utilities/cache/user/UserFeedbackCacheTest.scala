package com.gravity.utilities.cache.user

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest

/**
  * Created by akash on 8/23/16.
  */
class UserFeedbackCacheTest extends BaseScalaTest with operationsTesting {

  ignore("Read single normailization params from prod") {
    HBaseConfProvider.setAws()
    val res = UserFeedbackContentQualityCache.getNormalizationParams(4,0)
    println(res)
  }

  ignore("Read single calibration params from prod") {
    HBaseConfProvider.setAws()
    val res = UserFeedbackContentQualityCache.getCalibrationParams(1,0)
    println(res)
  }

  test("test valid param keys") {
    HBaseConfProvider.setAws()
    val keys = UserFeedbackContentQualityCache.validKeys
    println("keys: " + keys)
  }


  ignore("Read single p and theta from test path") {

    HBaseConfProvider.setUnitTest()

    val outputPath = "/user/gravity/reports/"
    val outputFile = "userfeedback_normalizationparameters.csv"

    withHdfsTestPath(outputPath) { basePath =>
      grvhadoop.withHdfsStreamWriter(HBaseConfProvider.getConf.fs, basePath + outputFile)(writer => {

        val data =  """userfeedbackvariationid,chosenuserfeedbackoptionid,p,theta
4,0,0.0022,13.08
4,10,0.1264,13.08
4,11,0.622,13.08
4,12,0.3558,13.08
4,13,0.2657,13.08
4,14,0.1276,13.08
5,0,0.8053,14.89
"""

        writer.write(data.getBytes())

      })

      grvhadoop.perHdfsLine(HBaseConfProvider.getConf.fs, basePath+outputFile)(line=>println(line))
    }


  }

}
