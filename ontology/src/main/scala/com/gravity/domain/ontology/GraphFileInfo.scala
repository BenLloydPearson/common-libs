package com.gravity.domain.ontology

import java.io.Serializable

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.apache.hadoop.fs.{FileUtil, Path}

/**
  * Created by akash on 8/9/16.
  */
case class GraphFileInfo(useTestFiles: Boolean) extends Serializable{

  val neo4JGraphDir = "/opt/interests/data2/graph.2015.04/"


  // hdfs root path for everything
  private val rootPath = "/user/gravity/ontology/"


  // ----------------- Input files --------------
  private val topicLbl = "labels_en.nt"
  private val categoryLbl = "category-labels_en.nt"
  private val topicToCategory = "article-categories_en.nt"
  private val skosCategory = "skos-categories_en.nt"

  // nytimes - same input file for all cases
  private val nyTimesDir = "nytimes/"
  private val nyTimesPeople = "people.nt"

  // prod input path
  private val dbpediaDir = "dbpedia.2015.04/"
  private val dbpediaPath = rootPath + dbpediaDir


  // test input path
  private val testDir = "sample.dbpedia/"
  private val dbpediaPathTest = rootPath + testDir


  // ----------------- Output files for raw ontology--------------
  private val vertexOutput = "bin.nodes"
  private val edgeOutput = "bin.edges"
  private val edgeOutputTxt = "grv.edges"

  // ----------------- Output files for full ontology--------------
  private val exportVertexOutput = "grv.nodes"
  private val exportEdgeOutput = "grv.edges2"

  private val masterNodeImportFileName = "import.grv.nodes"
  private val masterEdgeImportFileName = "import.grv.edges"

  // prod output path
  private val outputDir = "generated/"
  private val outputPath = rootPath + outputDir

  // test output path
  private val outputTestDir = "generated.test/"
  private val outputPathTest = rootPath + outputTestDir


  private def hdfsInputPath(fileName: String) = {
    if (useTestFiles) dbpediaPathTest + fileName
    else dbpediaPath + fileName
  }

  private def hdfsOutputPath(fileName: String) = {
    if (useTestFiles) outputPathTest + fileName
    else outputPath + fileName
  }

  def withTimestamp(fileName: String, ts: Long) = fileName + "." + ts

  lazy val topicFile = hdfsInputPath(topicLbl)
  lazy val categoryFile = hdfsInputPath(categoryLbl)
  lazy val topicToCategoryFile = hdfsInputPath(topicToCategory)
  lazy val skosCategoryFile = hdfsInputPath(skosCategory)
  lazy val nyTimesPeopleFile = rootPath + nyTimesDir + nyTimesPeople

  lazy val vertexFile = hdfsOutputPath(vertexOutput)
  lazy val edgeFile = hdfsOutputPath(edgeOutput)
  lazy val edgeFileTxt = hdfsOutputPath(edgeOutputTxt)

  lazy val exportVertexFile = hdfsOutputPath(exportVertexOutput)
  lazy val exportEdgeFile = hdfsOutputPath(exportEdgeOutput)


  def getFinalNodeImportFile(ts: Long) = {
    val dirPath  = withTimestamp( hdfsOutputPath(exportVertexOutput), ts)
    val filePath = withTimestamp( hdfsOutputPath(masterNodeImportFileName), ts)
    copyMerge(dirPath, filePath)
    filePath
  }

  def getFinalEdgeImportFile(ts: Long) = {
    val dirPath  = withTimestamp( edgeFileTxt, ts)
    val filePath = withTimestamp( hdfsOutputPath(masterEdgeImportFileName), ts)
    copyMerge(dirPath, filePath)
    filePath
  }


  def copyMerge(dirPath: String, filePath: String, deleteSource: Boolean = true) = {
    val conf = HBaseConfProvider.getConf
    val fs = conf.fs

    if (!fs.exists(new Path(filePath))){
      println("Performing copyMerge to generate file: " + filePath + " from dir: " + dirPath)
      FileUtil.copyMerge(fs, new Path(dirPath), fs, new Path(filePath), deleteSource, conf.defaultConf, null)
      println("CopyMerge complete.")
    }
    else{
      println("Skipping copyMerge since file exists: " + filePath)
    }
  }
}
