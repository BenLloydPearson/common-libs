package com.gravity.utilities.grvhadoop

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.interests.jobs.intelligence.helpers.grvhadoop
import com.gravity.test.operationsTesting
import com.gravity.utilities.BaseScalaTest
import org.apache.hadoop.fs.Path

/**
 * Created by bragazzi on 11/4/15.
 */
class grvhadoopTest extends BaseScalaTest with operationsTesting {

  def hdfsTestFiles(basePath: String) =
    new {
      val directory1 = new Path(basePath + "/directory1")
      val directory2 = new Path(basePath + "/directory2")
      val file1 = new Path(basePath + "/file1")
      val file2 = new Path(basePath + "/file2")

      HBaseConfProvider.getConf.fs.createNewFile(new Path(directory1 + "/dir1file"))
      HBaseConfProvider.getConf.fs.createNewFile(new Path(directory2 + "/dir2file"))
      HBaseConfProvider.getConf.fs.createNewFile(file1)
      HBaseConfProvider.getConf.fs.createNewFile(file2)
    }

  test("directoriesIn returns Paths for all directories and no files") {
    withHdfsTestPath("/user/gravity/test/reports/unit") { basePath =>
      val hdfs = hdfsTestFiles(basePath)

      val results = grvhadoop.directoriesIn(basePath)

      results.size should equal(2)
      results should contain(hdfs.directory1)
      results should contain(hdfs.directory2)
      results shouldNot contain(hdfs.file1)
      results shouldNot contain(hdfs.file2)
    }
  }

  test("filesIn returns Paths for all files and no directories") {
    withHdfsTestPath("/user/gravity/test/reports/unit") { basePath =>
      val hdfs = hdfsTestFiles(basePath)

      val results = grvhadoop.filesIn(basePath)

      results.size should equal(2)
      results shouldNot contain(hdfs.directory1)
      results shouldNot contain(hdfs.directory2)
      results should contain(hdfs.file1)
      results should contain(hdfs.file2)
    }
  }

}
