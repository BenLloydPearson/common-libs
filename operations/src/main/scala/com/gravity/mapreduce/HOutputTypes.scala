package com.gravity.mapreduce

import com.gravity.hbase.mapreduce.{SettingsBase, HOutput}
import org.apache.avro.Schema
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, MultipleOutputs, FileOutputFormat}
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
 * Created by cstelzmuller on 5/22/15.
 */
case class HMultiFileOutput(path: String) extends HOutput {
  override def init(job: Job, settings: SettingsBase) {
    FileSystem.get(job.getConfiguration).delete(new Path(path), true)
    FileOutputFormat.setOutputPath(job, new Path(path))
    MultipleOutputs.addNamedOutput(job, "text", classOf[TextOutputFormat[NullWritable, Text]], classOf[NullWritable], classOf[Text])
    MultipleOutputs.setCountersEnabled(job, false)
  }
}

case class HParquetFileOutput(path: String, schema: Schema) extends HOutput {
  override def init(job: Job, settings: SettingsBase) {
    FileSystem.get(job.getConfiguration).delete(new Path(path), true)
    job.setOutputFormatClass(classOf[AvroParquetOutputFormat])
    FileOutputFormat.setOutputPath(job, new Path(path))
    AvroParquetOutputFormat.setSchema(job, schema)
  }
}

case class HParquetMultiFileOutput(path: String, outputs: List[(String, Schema)], compression: String = "") extends HOutput {
  override def init(job: Job, settings: SettingsBase): Unit = {
    FileSystem.get(job.getConfiguration).delete(new Path(path), true)
    FileOutputFormat.setOutputPath(job, new Path(path))
    // first schema in the list is used as the default job output
    AvroParquetOutputFormat.setSchema(job, outputs.head._2)

    // add each named output
    outputs.foreach(item => {
      GrvAvroMultipleOutputs.addNamedOutput(job, item._1, classOf[AvroParquetOutputFormat], item._2, item._2)
    })

    GrvAvroMultipleOutputs.setCountersEnabled(job, true)

    // set compression for job
    compression match {
      case "snappy" => {
        FileOutputFormat.setCompressOutput(job, true)
        ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
      }
      case "gzip" => {
        FileOutputFormat.setCompressOutput(job, true)
        ParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP)
      }
      case _ => ParquetOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED)
    }
  }
}
