package com.gravity.mapreduce

import com.gravity.hbase.mapreduce.{HInput, SettingsBase, TextToBinaryMapper}
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs, TextInputFormat}
import org.apache.parquet.avro.AvroParquetInputFormat

import scala.collection.Map

/**
 * Created by cstelzmuller on 5/29/15.
 */
case class HParquetInput(paths: Seq[String]) extends HInput {
  override def init(job: Job, settings: SettingsBase) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
    job.setInputFormatClass(classOf[AvroParquetInputFormat[IndexedRecord]])
  }
}

case class HMultiFileInput(paths: Map[String, String]) extends HInput {
  override def init(job: Job, settings: SettingsBase) {
    paths.foreach(entry => {
      entry._2 match {
        case "text" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[TextInputFormat], classOf[TextToBinaryMapper])
        case "parquet" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[AvroParquetInputFormat[IndexedRecord]], classOf[ParquetToBinaryMapper])
        case _ => throw new IllegalArgumentException("Input Format Not Supported Yet")
      }
    })
  }
}

// this is one very hacky fix, but we'll leave it in until we get the jobs migrated to cascading
case class HMultiFileInput2(paths: Map[String, String]) extends HInput {
  override def init(job: Job, settings: SettingsBase) {
    paths.foreach(entry => {
      entry._2 match {
        case "text" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[TextInputFormat], classOf[TextToBinaryMapper])
        case "parquet-imp" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[GrvAvroParquetInputImpFormat], classOf[ParquetToBinaryMapper])
        case "parquet-view" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[GrvAvroParquetInputViewFormat], classOf[ParquetToBinaryMapper])
        case "parquet-click" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[GrvAvroParquetInputClickFormat], classOf[ParquetToBinaryMapper])
        case "parquet-beacon" => MultipleInputs.addInputPath(job, new Path(entry._1), classOf[GrvAvroParquetInputBeaconFormat], classOf[ParquetToBinaryMapper])
        case x => throw new IllegalArgumentException("Input Format " + x + " Not Supported Yet")
      }
    })
  }
}