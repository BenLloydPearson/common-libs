package com.gravity.interests.jobs.intelligence.helpers

import java.io._
import java.net.{URL, URLClassLoader}

import com.google.common.base.Charsets
import com.gravity.hadoop.RichFileSystem
import com.gravity.hbase.mapreduce.HConfigLet
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.Settings
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
//import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import org.apache.avro
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job

import scala.collection._
import scala.collection.mutable.Buffer
import scalaz.ValidationNel
import scalaz.syntax.validation._


case class ReducerCountConfInternal(reducers: Int = 1) extends HConfigLet {
  override def configure(job: Job) {
    if (!Settings.isTest) {
      job.setNumReduceTasks(reducers)
    } else {
      job.setNumReduceTasks(2)
    }
    job.getConfiguration.setInt("mapreduce.job.reduces", reducers)
  }
}


case class MemoryOverrideConf(mapMemoryMB: Int, reduceMemoryMB: Int, permSizeMB: Int = 96) extends HConfigLet {

  val usePhysical = false

  override def configure(job: Job) {
    val mapMemory = mapMemoryMB
    val mapVirtualMemory = mapMemoryMB + grvhadoop.MAP_VIRTUAL_OVERHEAD
    val reduceMemory = reduceMemoryMB
    val reduceVirtualMemory = reduceMemoryMB + grvhadoop.REDUCE_VIRTUAL_OVERHEAD

    val mapPhysicalMemory = mapMemoryMB + grvhadoop.MAP_PHYSICAL_OVERHEAD
    val reducePhysicalMemory = reduceMemoryMB + grvhadoop.REDUCE_PHYSICAL_OVERHEAD


    job.getConfiguration.set("mapred.map.child.java.opts", "-Xmx" + mapMemory + "m" + " -Xms" + mapMemory + "m -XX:-UseGCOverheadLimit -XX:PermSize=" + permSizeMB + "m")
    job.getConfiguration.set("mapreduce.map.java.opts", "-Xmx" + mapMemory + "m" + " -Xms" + mapMemory + "m -XX:-UseGCOverheadLimit -XX:PermSize=" + permSizeMB + "m")
    job.getConfiguration.set("mapred.reduce.child.java.opts", "-Xmx" + reduceMemory + "m -XX:-UseGCOverheadLimit -XX:PermSize=" + permSizeMB + "m")
    job.getConfiguration.set("mapreduce.reduce.java.opts", "-Xmx" + reduceMemory + "m -XX:-UseGCOverheadLimit -XX:PermSize=" + permSizeMB + "m")

    if(usePhysical) {
      job.getConfiguration.setInt("mapreduce.map.memory.physical.mb", mapPhysicalMemory)
      job.getConfiguration.setInt("mapreduce.reduce.memory.physical.mb",reducePhysicalMemory)
    }else {
      job.getConfiguration.setInt("mapred.job.map.memory.mb", mapVirtualMemory)
      job.getConfiguration.setInt("mapreduce.map.memory.mb", mapVirtualMemory)
      job.getConfiguration.setInt("mapred.job.reduce.memory.mb", reduceVirtualMemory)
      job.getConfiguration.setInt("mapreduce.reduce.memory.mb", reduceVirtualMemory)
    }
  }
}

case class WithCombiner(c: Class[_ <: org.apache.hadoop.mapreduce.Reducer[_, _, _, _]]) extends HConfigLet {

  override def configure(job: Job) {
    job.setCombinerClass(c)
  }

}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Things to wrap and assist with hadoop operations (that are not in hpaste).
 * This is in the Jobs project because the hadoop dependencies are large and not mandatory for the utilities
 * project.
 */
package object grvhadoop {

  val MAP_VIRTUAL_OVERHEAD = 2500
  val REDUCE_VIRTUAL_OVERHEAD = 2500
  val MAP_PHYSICAL_OVERHEAD = 1024
  val REDUCE_PHYSICAL_OVERHEAD = 1248

  def copyDir(sourcePath: Path, destinationPath: Path): Boolean = {
    val fs = HBaseConfProvider.getConf.fs

    HBaseConfProvider.getConf.fs.delete(destinationPath, true)
    FileUtil.copy(fs, sourcePath, fs, destinationPath, false, true, HBaseConfProvider.getConf.defaultConf)
  }

  def deletePath(path: Path): Boolean = {
    val p = path
    HBaseConfProvider.getConf.fs.delete(p, true)
  }

  def tryDeletePath(path: Path): Boolean = {
    try {
      deletePath(path)
    }
    catch{
      case ex: java.io.IOException =>
        println("Ignoring. Failed to delete " + path.toString + ". " +  ex)
        false
    }
  }


  def tryDelete(dirName: String): ValidationNel[FailureResult, Boolean] = {
    try {
      delete(dirName)
    }
    catch{
      case ex: java.io.IOException =>
        FailureResult("Failed to delete dir " + dirName.toString + ". " +  ex).failureNel
    }

  }

  def delete(dirName: String): ValidationNel[FailureResult, Boolean] = {
    val p = new Path(dirName)

    if (!HBaseConfProvider.getConf.fs.exists(p)) {
      FailureResult(dirName + " does not exist").failureNel
    }
    else if (HBaseConfProvider.getConf.fs.isDirectory(p)) {
      HBaseConfProvider.getConf.fs.delete(p, true).successNel
    } else {
      FailureResult("Is not a directory").failureNel
    }
  }

  def reportPath(dirName: String): String = "/gravity/reports/" + dirName

  def withSequenceFileWriter[K, V](fs: FileSystem, relpath: String, recreateIfPresent: Boolean = true)(key: K, value: V)(work: (SequenceFile.Writer, K, V) => Unit) {
    val path = new Path(relpath)
    if (recreateIfPresent) {
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }

    val w = SequenceFile.createWriter(HBaseConfProvider.getConf.defaultConf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass), SequenceFile.Writer.valueClass(value.getClass))
    work(w, key, value)

    w.close()
  }

  def pathAge(path: Path): Long = HBaseConfProvider.getConf.fs.getFileStatus(path).getModificationTime

  def pathExists(path: Path): Boolean = HBaseConfProvider.getConf.fs.exists(path)

  def testPath(path: String) {
    val p = new Path(path)
    if (HBaseConfProvider.getConf.fs.exists(p)) {}
    else HBaseConfProvider.getConf.fs.create(p)
  }

  def printFileList(relpath: String, printFileAsWell: Boolean = false) {
    val iter = HBaseConfProvider.getConf.fs.listFiles(new Path(relpath), true)
    while (iter.hasNext) {
      val itm = iter.next()
      if (itm.isDirectory) println("D" + itm.getPath.toString)
      if (itm.isFile) {
        println("F: " + itm.getPath.toString)
        if (printFileAsWell) {
          val file = HBaseConfProvider.getConf.fs.open(itm.getPath)
          val input = new BufferedReader(new InputStreamReader(file.getWrappedStream, Charsets.US_ASCII))
          var done = false
          while (!done) {
            val line = input.readLine()
            if (line == null)
              done = true
            else
              println(line)
          }
        }
      }
    }
  }

  def getFileList(relpath: String): List[String] = {
    val files = new scala.collection.mutable.ListBuffer[String]()
    val iter = HBaseConfProvider.getConf.fs.listFiles(new Path(relpath), true)
    while (iter.hasNext) {
      val itm = iter.next()
      if (itm.isDirectory) println("D" + itm.getPath.toString)
      if (itm.isFile) {
        files.append(itm.getPath.toString)
      }
    }
    files.toList
  }

  def retrieveTextFileContent(relpath: String, fileName: String): Iterator[String] = {
    val iter: RemoteIterator[LocatedFileStatus] = HBaseConfProvider.getConf.fs.listFiles(new Path(relpath), true)
    var lineSeq: Seq[String] = Seq()
    while (iter.hasNext) {
      val itm: LocatedFileStatus = iter.next()
      if (itm.isDirectory) println("D" + itm.getPath.toString)
      if (itm.isFile) {
        val fqfn = itm.getPath.toString
        val separator = "/"
        println("F: " + fqfn)
        val pieces: Array[String] = fqfn.split(separator)
        val ix = if (fqfn.endsWith(separator)) pieces.length - 2 else pieces.length - 1
        if (pieces(ix).equals(fileName)) {
          val file = HBaseConfProvider.getConf.fs.open(itm.getPath)
          val input = new BufferedReader(new InputStreamReader(file.getWrappedStream, Charsets.US_ASCII))
          var done = false
          while (!done) {
            val line = input.readLine()
            if (line == null)
              done = true
            else
              lineSeq = line +: lineSeq
          }
        }
      }
    }
    lineSeq.reverseIterator
  }
  /**
   * Gives you a file writer into the local cluster hdfs instance
 *
   * @param relpath The relative path
   * @param recreateIfPresent If true, will delete the file if it already exists
   * @param work A function that works with the output.  The output will be closed when this function goes out of scope.
   * @return
   */
  def withHdfsWriter(fs: FileSystem, relpath: String, recreateIfPresent: Boolean = true)(work: (BufferedWriter) => Unit) {
    val path = new Path(relpath)
    val fileSystem = fs
    if (recreateIfPresent) {
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, true)
      }
    }
    val output = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)))
    try {
      work(output)
    } finally {
      output.flush()
      output.close()
    }
  }
  def withHdfsStreamWriter(fs: FileSystem, relpath: String, recreateIfPresent: Boolean = true)(work: (FSDataOutputStream) => Unit) {
    val path = new Path(relpath)
    val fileSystem = fs
    if (recreateIfPresent) {
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, true)
      }
    }
    val output = fileSystem.create(path)
    try {
      work(output)
    } finally {
      output.flush()
      output.close()
    }
  }
  def withHdfsParquetAvroWriter[T <: IndexedRecord](fs: FileSystem, relPath: String, recreateIfPresent: Boolean = true)
                                                   (avroSchema: avro.Schema, compressionCodec: CompressionCodecName = CompressionCodecName.GZIP, blockSize: Int = 8 * math.pow(1024, 2).toInt, pageSize: Int = math.pow(1024, 2).toInt)
                                                   (work: (AvroParquetWriter[T]) => Unit): Unit = {
    val path = new Path(relPath)
    val fileSystem = fs
    if (recreateIfPresent) {
      if (fileSystem.exists(path)) {
        fileSystem.delete(path, true)
      }
    }

    val parquetWriter = new AvroParquetWriter[T](path, avroSchema, compressionCodec, blockSize, pageSize)
    try {
      work(parquetWriter)
    } finally {
      parquetWriter.close()
    }
  }

  def withHdfsParquetAvroReader[T <: IndexedRecord](relPath: String)
                                                   (work: (AvroParquetReader[T]) => Unit): Unit = {
    val path = new Path(relPath)
    val parquetReader = new AvroParquetReader[T](path) // this is deprecated, but the non-deprecated version has a bug in it, so waiting on 5.5 to move away from deprecated code
    try {
      work(parquetReader)
    } finally {
      parquetReader.close()
    }
  }

  def directoriesIn(path: String): Seq[Path] = {
    searchPath(path, fileStatus = { _.isDirectory })
  }

  def filesIn(path: String): Seq[Path] = {
    searchPath(path, fileStatus = { _.isFile })
  }

  def searchPath(path: String, fileStatus: FileStatus => Boolean = { _ => true }): Seq[Path] = {
    val p = new Path(path)
    val files = HBaseConfProvider.getConf.fs.listStatus(p)
    files.filter(fileStatus).map(_.getPath).toSeq
  }


  def perPartSequenceFileKV[K <: Writable, V <: Writable](fs: FileSystem, relpath: String, conf: Configuration, fileBeginsWith: String = "part-")(key: K, value: V)(line: (K, V) => Unit): ValidationNel[FailureResult, Seq[String]] = {
    val glob = new Path(relpath)

    val files = fs.listStatus(glob, new PathFilter {
      override def accept(path: Path): Boolean = {
        val result = path.getName.startsWith(fileBeginsWith)
        result
      }
    })

    (for (file <- files) yield {
      perSequenceFileKV(fs, file.getPath.toString, conf)(key, value)(line)
    }).toSeq.extrude
  }


  def perSequenceFileKV[K <: Writable, V <: Writable](fs: FileSystem, relpath: String, conf: Configuration)(key: K, value: V)(line: (K, V) => Unit): ValidationNel[FailureResult, String] = {
    try {
      val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(relpath)))


      try {
        while (reader.next(key, value)) {
          line(key, value)
        }
        ("Ran through " + relpath).successNel
      } catch {
        case ex: Exception => FailureResult("Unable to read sequence file: " + relpath, ex).failureNel
      } finally {
        reader.close()
      }
    } catch {
      case ex: FileNotFoundException => {
        FailureResult("File not found: " + relpath).failureNel
      }
    }
  }


  /**
   * Allows you to work with a reader opened into an hdfs file on the test cluster.
 *
   * @param relpath The path to the file
   * @param work The work you will do
   * @tparam A If you want to return a value after the work, here it is.
   * @return
   */
  def withHdfsReader[A](fs: FileSystem, relpath: String)(work: (BufferedReader) => A): A = {
    val path = new Path(relpath)
    val input = new BufferedReader(new InputStreamReader(fs.open(path)))

    try {
      work(input)
    } finally {
      input.close()
    }
  }

  def withHdfsDirectoryReader[A](fs: FileSystem, relpath: String)(work: (BufferedReader) => A): A = {
    val path = new Path(relpath)
    val input = new BufferedReader(new InputStreamReader(new RichFileSystem(fs).openParts(path)))
    try {
      work(input)
    } finally {
      input.close()
    }
  }

  def createFileFromStream(fs: FileSystem, relPath: String, is: InputStream): Unit = {
    withHdfsWriter(fs, relPath)(writer => {
      org.apache.commons.io.IOUtils.copy(is, writer)
      is.close()
    })
  }


  /**
   * Reads a file into a buffer, allowing you to decide what's in the buffer depending on the output of the linereader function
 *
   * @param relpath Path to local hdfs buffer
   * @param linereader Function to return an element in the buffer, given the line fo the file
   * @tparam A
   * @return
   */
  def perHdfsLineToSeq[A](fs: FileSystem, relpath: String)(linereader: (String) => A): Seq[A] = {
    val result = Buffer[A]()

    withHdfsReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            result += linereader(line)
          }
        }
    }
    result.toSeq
  }

  def perHdfsDirectoryLineToSeq[A](fs: FileSystem, relpath: String)(linereader: (String) => A): Seq[A] = {
    val result = Buffer[A]()

    withHdfsDirectoryReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            result += linereader(line)
          }
        }
    }
    result.toSeq
  }

  /**
   * Reads a file line by line.  If you want to have the results in a buffer, use perHdfsLineToSeq
 *
   * @param relpath
   * @param linereader
   * @tparam A
   * @return
   */
  def perHdfsLine[A](fs: FileSystem, relpath: String)(linereader: (String) => Unit) {
    withHdfsReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            linereader(line)
          }
        }
    }
  }

  def mergeFilesToLocal(sourceDirectory: String, localFile: String, replace: Boolean = true): Boolean = {
    val localFs = FileSystem.getLocal(HBaseConfProvider.getConf.defaultConf)
    val target = new Path(localFile)
    if (replace && localFs.exists(target)) {
      localFs.delete(target, false)
    }
    FileUtil.copyMerge(HBaseConfProvider.getConf.fs, new Path(sourceDirectory), FileSystem.getLocal(HBaseConfProvider.getConf.defaultConf), new Path(localFile), false, HBaseConfProvider.getConf.defaultConf, null)
  }

  /**
   * For each line in a directory of files
 *
   * @param relpath Path to files (or glob path)
   * @param linereader Will be invoked once per line with a string representation
   * @return Bupkiss
   */
  def perHdfsDirectoryLine(fs: FileSystem, relpath: String)(linereader: (String) => Unit) {
    withHdfsDirectoryReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            linereader(line)
          }
        }
    }
  }

  def countHdfsDirectoryLines(fs: FileSystem, relpath: String): Long = {
    var lines = 0L
    withHdfsDirectoryReader(fs, relpath) {
      input =>
        var done = false
        while (!done) {
          val line = input.readLine()
          if (line == null) {
            done = true
          } else {
            lines += 1
          }
        }
    }
    lines
  }

  def copyFileFromLocalToHdfs(clazz: Class[_], fs: FileSystem, localPath: String, hdfsPath: String) {
    val url = clazz.getResource(localPath).toURI
    fs.copyFromLocalFile(new Path(url), new Path(hdfsPath))
  }
//
//  /*
//    Execute a block of code with HADOOP_CLASSPATH classpath
//   */
//  def withHadoopClassloader[T](thunk: => T): T = {
//    val current = Thread.currentThread().getContextClassLoader
//    try {
//      current match {
//        case url: URLClassLoader => url.getURLs.foreach(u => println("parent: " + u.toString))
//        case _ =>
//      }
//      val hadoopClassloader = new URLClassLoader(hadoopClasspath, current)
//      hadoopClassloader match {
//        case url: URLClassLoader => url.getURLs.foreach(u => println("hadoop: " + u.toString))
//        case _ =>
//      }
//      Thread.currentThread().setContextClassLoader(hadoopClassloader)
//      thunk
//    } finally {
//      Thread.currentThread().setContextClassLoader(current)
//    }
//  }
//
//  lazy val hadoopClasspath = {
//    Option(System.getenv("HADOOP_CLASSPATH")).filter(_.nonEmpty).toSeq.flatMap(_.split(':')).flatMap(path => {
//      if (path.endsWith("*")) {
//        new File(path.stripSuffix("*")).listFiles(new FileFilter {
//          override def accept(pathname: File): Boolean = pathname.getName.endsWith(".jar") || pathname.getName.endsWith(".xml") || pathname.getName.endsWith(".properties") || pathname.getName.endsWith(".class")
//        }).toSeq
//      } else {
//        Seq(new File(path))
//      }
//    }).map(_.toURI.toURL).toArray
//  }

}
