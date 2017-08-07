package com.gravity.interests.jobs.intelligence.helpers

// JE: commented out until we figure out the dependency problem
//import cascading.jdbc.{JDBCScheme, JDBCTap}

import java.net.URLDecoder
import java.util
import java.util.Map.Entry

import cascading.flow.hadoop2.Hadoop2MR1FlowConnector
import cascading.flow.{Flow, FlowProcess}
import cascading.operation._
import cascading.operation.aggregator.Count
import cascading.pipe._
import cascading.pipe.assembly.{Retain, Unique}
import cascading.pipe.joiner.Joiner
import cascading.property.AppProps
import cascading.scheme.Scheme
import cascading.scheme.hadoop.{SequenceFile, TextDelimited, TextLine, WritableSequenceFile}
import cascading.tap.hadoop._
import cascading.tap.partition.{DelimitedPartition, Partition}
import cascading.tap.{MultiSourceTap, SinkMode, Tap}
import cascading.tuple.{Fields, Tuple, TupleEntry}
import com.gravity.cascading.hbase.{GrvHBaseRowScheme, GrvHBaseRowTap}
import com.gravity.hbase.schema._
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.mapreduce.{GrvChillKryoInstantiator, GrvParquetAvroScheme}
import com.gravity.utilities.{Settings, grvtime}
import com.twitter.chill.config.ConfiguredInstantiator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.mapred.{OutputCollector, RecordReader}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

trait CascadingHelp {
  type SchemeTemplate = Scheme[Configuration, RecordReader[_ <: Any, _ <: Any], OutputCollector[_ <: Any, _ <: Any], _ <: Any, _ <: Any]
  type TapAlias = Tap[_ <: Any, _ <: Any, _ <: Any]

}

package object grvcascading extends CascadingHelp {

  implicit def toFieldMaker(strings: Seq[String]): FieldMaker = new FieldMaker(strings)


  type WorkSpec = (FlowProcess[_], FunctionCall[Any]) => Unit
  type RowWorkSpec[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] = (RR, FlowProcess[_], FunctionCall[Any]) => Unit
  type BufferSpec = (FlowProcess[_], BufferCall[Any]) => Unit
  type FilterSpec = (FlowProcess[_], FilterCall[Any]) => Boolean
  type TableSpec[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] = T

  implicit class RichAssembly(val pipe: Pipe) extends AnyVal {
    def each(work: WorkSpec)(outputfields: String*): Each = {
      new Each(pipe, new GrvFunc(outputfields: _*)(work))
    }
    def each2(work: WorkSpec)(outputfields: Fields): Each = {
      new Each(pipe, new GrvFunc2(outputfields)(work))
    }

    def eachFields(work: WorkSpec)(outputfields: String*)(finalOutputFields: String*): Each = {
      new Each(pipe, new GrvFunc(outputfields: _*)(work), new Fields(finalOutputFields: _*))
    }


    def eachArgs(arguments: String*)(work: WorkSpec)(outputFields: String*): Each = {
      new Each(pipe, arguments.toFields, new GrvFunc(outputFields: _*)(work))
    }

    def eachRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])(work: RowWorkSpec[T, R, RR])(outputfields: String*): Each = {
      new Each(pipe, new GrvRowFunc(table)(outputfields: _*)(work))
    }

    def groupByAndSort(groupFields: String*)(sortFields: String*)(descending: Boolean = false): GroupBy = {
      new GroupBy(pipe, groupFields.toFields, sortFields.toFields, descending)
    }

    def groupBy(groupFields: String*): GroupBy = {
      new GroupBy(pipe, groupFields.toFields)
    }


    def reduceBuffer(work: BufferSpec)(outputfields: String*): Every = {
      new Every(pipe, new GrvReducerBuffer(outputfields: _*)(work))
    }

    def reduceBufferArgs(arguments: String*)(work: BufferSpec)(outputfields: String*)(finalOutputFields: String*): Every = {
      new Every(pipe, arguments.toFields, new GrvReducerBuffer(outputfields: _*)(work), finalOutputFields.toFields)
    }

    def count(countField: String*): Every = {
      new Every(pipe, new Count(countField.toFields))
    }

    def countArgs(argumentFields: String*)(countField: String*)(outFields: String*): Every = {
      new Every(pipe, argumentFields.toFields, new Count(countField.toFields), outFields.toFields)
    }


    def filter(work: FilterSpec): Each = {
      new Each(pipe, new GrvFilter(work))
    }

  }

  implicit class RichTap(val tap: Tap[_, _, _]) extends AnyVal

  trait DefaultProvider[T] {
    def default:T
  }

  implicit object StringDefault extends DefaultProvider[String] {
    def default = ""
  }
  implicit object JavaLongDefault extends DefaultProvider[java.lang.Long] {
    def default = 0l.asInstanceOf[java.lang.Long]
  }
  implicit object JavaIntDefault extends DefaultProvider[java.lang.Integer] {
    def default = 0.asInstanceOf[java.lang.Integer]
  }
  implicit object JavaShortDefault extends DefaultProvider[java.lang.Short] {
    def default = 0.asInstanceOf[java.lang.Short]
  }
  implicit object JavaDoubleDefault extends DefaultProvider[java.lang.Double] {
    def default = 0.0.asInstanceOf[java.lang.Double]
  }
  implicit object JavaFloatDefault extends DefaultProvider[java.lang.Float] {
    def default = 0.0.asInstanceOf[java.lang.Float]
  }

  implicit object IntDefault extends DefaultProvider[Int] {
    def default = 0
  }
  implicit object LongDefault extends DefaultProvider[Long] {
    def default = 0l
  }
  implicit object DoubleDefault extends DefaultProvider[Double] {
    def default = 0.0
  }

  object KittenStringDefault extends DefaultProvider[String] {
    def default = "kitten"
  }

  def considerNotNullValue(test: Any): Boolean = test match {
    case null => false
    case Nil => false
    case x: String => !(Vector(null, "").contains(x))
    case x: java.lang.Long => x != 0L.asInstanceOf[java.lang.Long]
    case x: java.lang.Integer => x != 0.asInstanceOf[java.lang.Integer]
    case x: java.lang.Short => x != 0.asInstanceOf[java.lang.Short]
    case x: java.lang.Double => x != 0.0.asInstanceOf[java.lang.Double]
    case x: java.lang.Float => x != 0.0.asInstanceOf[java.lang.Float]
    case x: java.lang.Boolean => true
    case x: Traversable[_] => true
    case x: DateTime => x != grvtime.epochDateTime
    case x: scala.collection.convert.Wrappers.IterableWrapper[_] => true
    case _ =>
      throw new Exception("grvcascading.considerNotNullValue: Unrecognized Type Encountered for " + test.getClass.getCanonicalName)
  }
  implicit class RichOperationCall(val call: OperationCall[Any]) extends AnyVal {
    def isUsable[T](test: T): Boolean = considerNotNullValue(test)

    /**
     *
     * @param fieldNames
     * @tparam T
     * @example
     *          coalesceD[java.lang.Long]("field1", "field2")
     *          coalesceD("field1", "field2")(KittenStringDefault)
     * @return T
     */
    def coalesceD[T : DefaultProvider](fieldNames: String*): T = {
      coalesce(fieldNames: _*)(implicitly[DefaultProvider[T]].default, isUsable _)
    }

    /**
     * The above method's operator T : DefaultProvider and then the implicitly call is syntactic sugar for the below approach with implicit param.
      *
      * @param fieldNames
     * @param ev
     * @tparam T
     * @return
     */
    def coalesceD_NoSyntacticSugar[T](fieldNames: String*)(implicit ev: DefaultProvider[T]): T = {
      coalesce(fieldNames: _*)(ev.default, isUsable _)
    }

    def coalesce[T](fieldNames: String*)(defaultValue: T, isUsableValue: (T) => Boolean = isUsable _): T = {
      for {
        fn <- fieldNames
      } {
        Option(call.asInstanceOf[ConcreteCall[Any]].getArguments.getObject(fn)) match {
          case Some(x) => {
            if (isUsableValue(x.asInstanceOf[T]))
              return x.asInstanceOf[T]
          }
          case _ => {}
        }
      }
      return defaultValue
    }

    def coalesceUnTyped(fieldNames: String*)(isUsableValue: AnyRef => Boolean = considerNotNullValue _): AnyRef = {
      for {
        fn <- fieldNames
      } {
        Option(call.asInstanceOf[ConcreteCall[Any]].getArguments.getObject(fn)) match {
          case Some(x) => {
            if (isUsableValue(x))
              return x
          }
          case _ => {}
        }
      }

      return call.asInstanceOf[ConcreteCall[Any]].getArguments.getObject(fieldNames.last)
    }

  }

  implicit class RichFunctionCall(val call: FunctionCall[Any]) extends AnyVal {
    def getRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]): RR = {
      call.getArguments.getRow(table)
    }

    def addTuple(tuple: (Tuple) => Unit) {
      val t = new Tuple()
      tuple(t)
      call.getOutputCollector.add(t)
    }

    def writeOp[T <: HbaseTable[T, R, _], R](op: OpBase[T, R]) {
      val tableBytes = new ImmutableBytesWritable(op.table.tableName.getBytes("UTF-8"))
      op.getOperations.foreach {
        op =>
          call.getOutputCollector.add(new Tuple(tableBytes, op))
      }
    }

    def write(items: AnyRef*) {
      call.getOutputCollector.add(new Tuple(items: _*))
    }

    def get[T](field: String): T = call.getArguments.getObject(field).asInstanceOf[T]

    def getObj[T](field: String)(implicit c: ByteConverter[T]): T = call.getArguments.getObj[T](field)(c)

  }


  implicit class RichFilterCall(val call: FilterCall[Any]) extends AnyVal {

    def getRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]): RR = {
      call.getArguments.getRow(table)
    }

    def get[T](field: String): T = call.getArguments.getObject(field).asInstanceOf[T]

    def getObj[T](field: String)(implicit c: ByteConverter[T]): T = call.getArguments.getObj[T](field)(c)

  }

  implicit class RichFlowProcess(val flow: FlowProcess[_]) extends AnyVal {
    def ctr(name:String): Unit = flow.increment("Custom",name,1l)
    def ctr(name:String, by:Long): Unit = flow.increment("Custom",name, by)
  }

  implicit class RichBufferCall(val call: BufferCall[Any]) extends AnyVal {
    def writeOp[T <: HbaseTable[T, R, _], R](op: OpBase[T, R]) {
      val tableBytes = new ImmutableBytesWritable(op.table.tableName.getBytes("UTF-8"))
      op.getOperations.foreach {
        op =>
          call.getOutputCollector.add(new Tuple(tableBytes, op))
      }
    }

    def write(items: AnyRef*) {
      call.getOutputCollector.add(new Tuple(items: _*))
    }

    def addTuple(tuple: (Tuple) => Unit) {
      val t = new Tuple()
      tuple(t)
      call.getOutputCollector.add(t)
    }

  }

  implicit class RichTupleEntry(val tuple: TupleEntry) extends AnyVal {
    def getF[T](field: String): T = tuple.getObject(field).asInstanceOf[T]

    def getObj[T](offSet: String)(implicit c: ByteConverter[T]): T = {

      val bw = tuple.getObject(offSet).asInstanceOf[BytesWritable]
      if (bw == null) return null.asInstanceOf[T]
      c.fromBytesWritable(bw)

    }

    def getRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]): RR = {
      table.buildRow(tuple.getObject("v").asInstanceOf[Result])
    }

  }

  implicit class RichTuple(val tuple: Tuple) extends AnyVal {
    def addObj[T](value: T)(implicit c: ByteConverter[T]): RichTuple = {
      tuple.add(c.toBytesWritable(value))
      this
    }
  }

  def confToProps(conf: Configuration): util.HashMap[Object, Object] = {
    val jprops = new java.util.HashMap[Object, Object]()

    conf.foreach {
      case entry: Entry[String, String] =>
        jprops.put(entry.getKey, entry.getValue)
    }

    jprops
  }

  def isValidPath(path: String): Boolean = {
    val badPaths = List(
                        "/user",
                        "/user/gravity",
                        "/user/gravity/logs",
                        "/user/gravity/logs/beacon",
                        "/user/gravity/logs/clickEvent",
                        "/user/gravity/logs/clickEvent-redirect",
                        "/user/gravity/logs/impressionServed",
                        "/user/gravity/logs/impressionViewedEvent",
                        "/user/gravity/logs/validatedBeacons",
                        "/user/gravity/logs/validatedConversions",
                        "/user/gravity/logs/widgetDomReadyEvent",
                        "/user/gravity/logs/RecoFailure",
                        "/user/gravity/logs.avro",
                        "/user/gravity/logs.avro/beacon",
                        "/user/gravity/logs.avro/clickEvent",
                        "/user/gravity/logs.avro/clickEvent.new",
                        "/user/gravity/logs.avro/clickEvent-redirect",
                        "/user/gravity/logs.avro/impressionServed",
                        "/user/gravity/logs.avro/impressionViewedEvent",
                        "/user/gravity/logs.avro/validatedBeacons",
                        "/user/gravity/logs.avro/validatedBeacons.new",
                        "/user/gravity/logs.avro/validatedConversions",
                        "/user/gravity/logs.avro/widgetDomReadyEvent",
                        "/user/gravity/logs.avro/RecoFailure",
                        "/user/gravity/reports",
                        "/user/gravity/reports/advertiserCampaignArticleReport",
                        "/user/gravity/reports/advertiserCampaignReport",
                        "/user/gravity/reports/campaignMetricsReport",
                        "/user/gravity/reports/cleansing",
                        "/user/gravity/reports/cleansing/click",
                        "/user/gravity/reports/cleansing/impressionServed",
                        "/user/gravity/reports/cleansing/impressionViewed",
                        "/user/gravity/reports/clickDepthReport",
                        "/user/gravity/reports/conversionReport",
                        "/user/gravity/reports/datamarts",
                        "/user/gravity/reports/datamarts/campaignArticle",
                        "/user/gravity/reports/datamarts/campaignReco",
                        "/user/gravity/reports/datamarts/geoDevice",
                        "/user/gravity/reports/dimensions",
                        "/user/gravity/reports/sitePlacementReport",
                        "/user/gravity/reports/sitePlacementTopArticleReportOrganic",
                        "/user/hive",
                        "/user/hive/warehouse",
                        "/user/hive/warehouse/publishermetrics",
                        "/user/gravity/reports/parquet",
                        "/user/gravity/reports/parquet/cleansing",
                        "/user/gravity/reports/parquet/cleansing/beacon",
                        "/user/gravity/reports/parquet/cleansing/click",
                        "/user/gravity/reports/parquet/cleansing/conversion",
                        "/user/gravity/reports/parquet/cleansing/impressionServed",
                        "/user/gravity/reports/parquet/cleansing/impressionViewed",
                        "/user/gravity/reports/parquet/datamarts",
                        "/user/gravity/reports/parquet/datamarts/article",
                        "/user/gravity/reports/parquet/datamarts/campaignAttributes",
                        "/user/gravity/reports/parquet/datamarts/reco",
                        "/user/gravity/reports/parquet/datamarts/unitImpression"
                      )

    !badPaths.contains(path)
  }

  // NOTE: we default to cacheBlocks=false here because this is a job-specific API, which should have caching turned off
  // http://hbase.apache.org/book/perf.reading.html
  def fromTable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR], maxVersions: Int = 1, cacheBlocks: Boolean = false, cacheSize: Int = 100)(query: (Query2Builder[T, R, RR]) => Query2[T, R, RR]): TapAlias = {
    val q = query(table.query2)

    new GrvHBaseRowTap(table.tableName, new GrvHBaseRowScheme(), q.makeScanner(maxVersions, cacheBlocks, cacheSize)).asInstanceOf[Tap[_, _, _]]
  }
/* JE: commented out until we figure out the dependency problem
  def fromMySqlQuery(conSettings: ConnectionSettings, selectQuery: String, countQuery: String)(fields: String*): TapAlias = {
    val driver = "com.mysql.jdbc.Driver"
    val sourceScheme = new JDBCScheme(fields.toFields, fields.toArray, selectQuery, countQuery)
    val source = new JDBCTap(conSettings.jdbcUrl, conSettings.username, conSettings.password, driver, sourceScheme).asInstanceOf[Tap[_, _, _]]
    source
  }
*/
  def toTables: TapAlias = {
    val sink = new GrvHBaseRowTap(new GrvHBaseRowScheme())
    sink.asInstanceOf[Tap[_, _, _]]
  }

  def toTextDelimited(path: String, delimiter: String = "\t")(fields: String*): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new TextDelimited(fields.toFields, delimiter).asInstanceOf[SchemeTemplate]
    val sink = new Hfs(sinkScheme, path, SinkMode.REPLACE).asInstanceOf[Tap[_, _, _]]
    sink
  }

  def toSequenceFileWritable(path: String, keyType: Class[_ <: Writable], valueType: Class[_ <: Writable])(keyField: String, valueField: String): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new WritableSequenceFile(Seq(keyField, valueField).toFields, keyType, valueType).asInstanceOf[SchemeTemplate]
    val sink = new Hfs(sinkScheme, path, SinkMode.REPLACE).asInstanceOf[Tap[_, _, _]]
    sink
  }

  def toSequenceFile(path: String)(fields: String*): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new SequenceFile(fields.toFields).asInstanceOf[SchemeTemplate]
    val sink = new Hfs(sinkScheme, path, SinkMode.REPLACE).asInstanceOf[Tap[_, _, _]]
    sink
  }


  def toPartitionSequenceFiles(path: String, partition: Partition, keyType: Class[_ <: Writable], valueType: Class[_ <: Writable])(pathFields: String*)(textFields: String*): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new WritableSequenceFile(textFields.toFields, keyType, valueType)
    val sink = new Hfs(sinkScheme, path)
    val partitionTap = new PartitionTap(sink, partition, SinkMode.REPLACE)

    partitionTap: TapAlias
  }

  def toPartitionSequenceFiles(path: String, keyType: Class[_ <: Writable], valueType: Class[_ <: Writable])(pathFields: String*)(textFields: String*): TapAlias = {
    toPartitionSequenceFiles(path, new DelimitedPartition(pathFields.toFields), keyType, valueType)(pathFields:_*)(textFields:_*)
  }

  def toPartitionFiles(path: String, partition: Partition)(pathFields: String*)(textFields: String*): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new TextDelimited(textFields.toFields).asInstanceOf[SchemeTemplate]
    val sink = new Hfs(sinkScheme, path)
    val partitionTap = new PartitionTap(sink, partition, SinkMode.REPLACE)
    partitionTap
  }

  def toPartitionFiles(path: String)(pathFields: String*)(textFields: String*): TapAlias = {
    toPartitionFiles(path, new DelimitedPartition(pathFields.toFields))(pathFields:_*)(textFields:_*)
  }

  def toTextFiles(path: String): TapAlias = {
    require(isValidPath(path))
    val sinkScheme = new TextDelimited(Fields.ALL).asInstanceOf[SchemeTemplate]
    val sink = new Hfs(sinkScheme, path, SinkMode.REPLACE).asInstanceOf[Tap[_, _, _]]
    sink
  }

  def fromSequenceGlob(path: String)(keyField: String, valueField: String)(keyClass: Class[_ <: Writable], valueClass: Class[_ <: Writable]): TapAlias = {
    val sourceScheme = new WritableSequenceFile(new Fields(keyField, valueField), keyClass, valueClass)
    val source = new GlobHfs(sourceScheme, path).asInstanceOf[Tap[_, _, _]]
    source
  }


  def fromGlobDelimited(path: String, delimiter: String = "\t")(fields: String*): TapAlias = {
    val sourceScheme = new TextDelimited(fields.toFields, delimiter)
    val source = new GlobHfs(sourceScheme, path).asInstanceOf[Tap[_, _, _]]
    source
  }

  def fromGlobLine(path: String)(field: String): TapAlias = {
    val sourceScheme = new TextLine(new Fields(field)).asInstanceOf[SchemeTemplate]
    val source = new GlobHfs(sourceScheme, path).asInstanceOf[Tap[_, _, _]]
    source
  }

  def fromGlobLineMulti(paths: Seq[String])(field: String): TapAlias = {
    val sourceScheme = new TextLine(new Fields(field)).asInstanceOf[SchemeTemplate]
    val sources = paths.map(sourcePath => new GlobHfs(sourceScheme, sourcePath).asInstanceOf[Tap[_, _, _]])
    val multiSource = new MultiSourceTap(sources: _*)
    multiSource
  }

  def fromAvroMulti(paths: Seq[String]): TapAlias = {
    val sourceScheme = new GrvParquetAvroScheme().asInstanceOf[SchemeTemplate]
    val sources = paths.map(sourcePath => new GlobHfs(sourceScheme, sourcePath).asInstanceOf[Tap[_, _, _]])
    val multiSource = new MultiSourceTap(sources: _*)
    multiSource
  }


  def fieldsToList(f: Fields): List[String] = {
    f.iterator.toList.asInstanceOf[List[String]]
  }
  def fieldsToVector(f: Fields): Vector[String] = {
    f.iterator.toVector.asInstanceOf[Vector[String]]
  }

  def fieldsToSeq(f: Fields): Seq[String] = fieldsToList(f).toSeq

  def fieldsToArray(f: Fields): Array[String] = fieldsToList(f).toArray

  def pipe(name: String): Pipe = new Pipe(name)

  def pipe(name: String, previous: Pipe): Pipe = new Pipe(name, previous)

  def coGroup(lhs: Pipe, lfields: List[String], rhs: Pipe, rfields: List[String], joiner: Joiner): CoGroup = {
    new CoGroup(lhs, lfields.toFields, rhs, rfields.toFields, joiner)
  }

  def coGroup(lhs: Pipe, lfields: List[String], rhs: Pipe, rfields: List[String]): CoGroup = {
    new CoGroup(lhs, lfields.toFields, rhs, rfields.toFields)
  }

  def flowWithOverridesForParquet(reducerCount: Int = 200, mapperMB: Int = 768, reducerMB: Int = 768, mapperVM: Int = 2048, reducerVM: Int = 2048,
                        combinedSplitMaxSizeInBytes: Option[Long] = None, props: Map[Object, Object] = Map.empty, debugLevel: DebugLevel = DebugLevel.DEFAULT,
                        permSizeMB: Int = 96, parquetCompression: CompressionCodecName = CompressionCodecName.GZIP,
                        parquetBlockSizeBytes: Long = (256*1024*1024).toLong, parquetPageSizeBytes: Long = (2*1024*1024).toLong,
                        dfsBlockSizeBytes: Long = (256*1024*1024).toLong): Hadoop2MR1FlowConnector = {

    val props_new = Map[Object, Object](
      "parquet.compression" -> parquetCompression.name(),
      "parquet.block.size" -> parquetBlockSizeBytes.toString,
      "parquet.page.size" -> parquetPageSizeBytes.toString,
      "dfs.blocksize" -> dfsBlockSizeBytes.toString
    ) ++ props

    flowWithOverrides(reducerCount=reducerCount, mapperMB=mapperMB, reducerMB=reducerMB, mapperVM=mapperVM,
      reducerVM=reducerVM, combinedSplitMaxSizeInBytes=combinedSplitMaxSizeInBytes, props=props_new, debugLevel=debugLevel,
      permSizeMB=permSizeMB, parquetCompression=parquetCompression)

  }
  def flowWithOverrides(reducerCount: Int = 200, mapperMB: Int = 768, reducerMB: Int = 768, mapperVM: Int = 2048, reducerVM: Int = 2048,
                        combinedSplitMaxSizeInBytes: Option[Long] = None, props: Map[Object, Object] = Map.empty, debugLevel: DebugLevel = DebugLevel.DEFAULT,
                        permSizeMB: Int = 96, parquetCompression: CompressionCodecName = CompressionCodecName.UNCOMPRESSED): Hadoop2MR1FlowConnector = {

    val jprops = grvcascading.confToProps(HBaseConfProvider.getConf.defaultConf)
    jprops.put("io.serializations", jprops.get("io.serializations") + ",com.twitter.chill.hadoop.KryoSerialization")
    jprops.put(ConfiguredInstantiator.KEY, classOf[GrvChillKryoInstantiator].getName)


    if (Settings.isProductionServer) {
      jprops.put("mapred.map.child.java.opts", "-Xmx" + mapperMB + "m -server -Djava.net.preferIPv4Stack=true -XX:PermSize=" + permSizeMB + "m") // deprecated in yarn
      jprops.put("mapreduce.map.java.opts", "-Xmx" + mapperMB + "m -server -Djava.net.preferIPv4Stack=true -XX:PermSize=" + permSizeMB + "m")
      jprops.put("mapred.reduce.child.java.opts", "-Xmx" + reducerMB + "m -server -Djava.net.preferIPv4Stack=true -XX:PermSize=" + permSizeMB + "m") // deprecated in yarn
      jprops.put("mapreduce.reduce.java.opts", "-Xmx" + reducerMB + "m -server -Djava.net.preferIPv4Stack=true -XX:PermSize=" + permSizeMB + "m")

      println("SUBMITTING WITH " + reducerMB + " for the java opts")

      jprops.put("mapred.job.map.memory.mb", mapperVM.toString) // deprecated in yarn
      jprops.put("mapreduce.map.memory.mb", mapperVM.toString)
      jprops.put("mapred.job.reduce.memory.mb", reducerVM.toString) // deprecated in yarn
      jprops.put("mapreduce.reduce.memory.mb", reducerVM.toString)
      jprops.put("mapred.compress.map.output", "true") // deprecated in yarn
      jprops.put("mapreduce.map.output.compress", "true")
      jprops.put("mapred.output.compression.type", "BLOCK") // deprecated in yarn
      jprops.put("mapreduce.output.fileoutputformat.compress.type", "BLOCK")
      jprops.put("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec") // deprecated in yarn
      jprops.put("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
      jprops.put("mapred.map.tasks.speculative.execution", "false") // deprecated in yarn
      jprops.put("mapreduce.map.speculative", "false")
      jprops.put("mapred.reduce.tasks", reducerCount.toString) // this is deprecated in yarn
      jprops.put("mapreduce.job.reduces", reducerCount.toString)

      println("SUBMITTING REDUCER COUNT OF " + reducerCount)
      jprops.put("hbase.regionserver.lease.period", "900000")
      jprops.put("hbase.rpc.timeout", "900000")
      jprops.put("parquet.compression", parquetCompression.name())

      combinedSplitMaxSizeInBytes.foreach(maxSize => {
        HfsProps.setUseCombinedInput(jprops, true)
        HfsProps.setCombinedInputMaxSize(jprops, maxSize)
      })
    } else {
      println("NOT SUBMITTING HADOOP CREDENTIALS")
      jprops.put("cascading.stats.complete_child_details.block.duration","0") //This keeps cascading from waiting 60 seconds for the jobs to finish.  We don't quite understand why it does that yet, Brian is investigating.  We're setting this in unit test mode.
    }

    props.foreach(prop => {
      jprops.put(prop._1, prop._2)
    })

    AppProps.setApplicationJarClass(jprops, classOf[GrvFunc])

    val flowConnector = new Hadoop2MR1FlowConnector(jprops)
    flowConnector
  }

  def flow: Hadoop2MR1FlowConnector = {
    flowWithOverrides()
  }

  val FIELDS_ALL: String = "ALL"
  val FIELDS_ARG: String = "ARGS"
  val FIELDS_REPLACE: String = "REPLACE"
  val FIELDS_VALUES: String = "VALUES"
  val FIELDS_RESULTS: String = "RESULTS"
  val FIELDS_SWAP: String = "SWAP"
  val ALL_FIELDS_INST: Fields = Seq("ALL").toFields
  val ARGS_FIELDS_INST: Fields = Seq("ARGS").toFields
}

import com.gravity.interests.jobs.intelligence.helpers.grvcascading._

abstract class GrvFuncBase(fields: String*) extends BaseOperation[Any](new Fields(fields: _*)) with cascading.operation.Function[Any] {

}


class GrvFunc2(fields: Fields)(work: WorkSpec) extends BaseOperation[Any](fields) with cascading.operation.Function[Any] {
  def operate(p1: FlowProcess[_], p2: FunctionCall[Any]) {
    work(p1, p2)
  }
}
class GrvFunc(fields: String*)(work: WorkSpec) extends BaseOperation[Any](new Fields(fields: _*)) with cascading.operation.Function[Any] {
  def operate(p1: FlowProcess[_], p2: FunctionCall[Any]) {
    work(p1, p2)
  }
}

class GrvRowFunc[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])(fields: String*)(work: RowWorkSpec[T, R, RR]) extends GrvFunc(fields: _*)((flow, call) => {
  val result = call.getArguments.getObject("v").asInstanceOf[Result]
  val row = table.buildRow(result)
  work(row, flow, call)
})


//class GrvRowFunc[T <: HbaseTable[T,R,RR],R, RR <: HRow[T,R]](table:HbaseTable[T,R,RR])(fields:String*)(work:grvcascading.RowWorkSpec[T,R,RR]) extends GrvFunc(fields:_*)((flow,call)=>{
//  val result = call.getArguments.getObject("v").asInstanceOf[Result]
//  val row = table.buildRow(result)
//  work(row, flow, call)
//})

class GrvFilter(work: FilterSpec) extends BaseOperation[Any] with cascading.operation.Filter[Any] {
  def isRemove(p1: FlowProcess[_], p2: FilterCall[Any]): Boolean = {
    work(p1, p2)
  }
}

class GrvReducerBuffer(fields: String*)(work: BufferSpec) extends BaseOperation[Any](new Fields(fields: _*)) with cascading.operation.Buffer[Any] {
  def operate(p1: FlowProcess[_], p2: BufferCall[Any]) {
    work(p1, p2)
  }
}
class GrvReducerBuffer2(fields: Fields)(work: BufferSpec) extends BaseOperation[Any](fields) with cascading.operation.Buffer[Any] {
  def operate(p1: FlowProcess[_], p2: BufferCall[Any]) {
    work(p1, p2)
  }
}


class FieldMaker(strings: Seq[String]) {
  def toFields: Fields = {
    strings.headOption match {
      case Some(grvcascading.FIELDS_ALL) => Fields.ALL
      case Some(grvcascading.FIELDS_ARG) => Fields.ARGS
      case Some(grvcascading.FIELDS_REPLACE) => Fields.REPLACE
      case Some(grvcascading.FIELDS_VALUES) => Fields.VALUES
      case Some(grvcascading.FIELDS_RESULTS) => Fields.RESULTS
      case Some(grvcascading.FIELDS_SWAP) => Fields.SWAP
      case _ => new Fields(strings: _*)
    }
  }
}

class MultiFieldUnique(var assembly: Pipe, iFields: Fields, oField: String) extends SubAssembly {
  assembly = new Retain(assembly, iFields)
  assembly = new Unique(assembly, iFields)

  assembly = new Each(assembly, new GrvFunc(oField)((flow, call) => {
    for (field <- fieldsToList(iFields)) {
      val tuple = new Tuple()
      tuple.add(call.getArguments.getString(field))
      call.getOutputCollector.add(tuple)
    }
  }))

  assembly = new Unique(assembly, new Fields(oField))
  setTails(assembly)
}

class FieldExpander(var assembly: Pipe, field: String, oFields: Fields, delimiter: String, urlEncoded: Boolean = false) extends SubAssembly {
  val expFieldsStr: scala.Seq[String] = grvcascading.fieldsToSeq(oFields)

  assembly = new Each(assembly, new GrvFunc(expFieldsStr: _*)((flow, call) => {
    flow.ctr("Field Expander: " + field)
    val tuple = new Tuple()
    var pField = Option(call.getArguments.getString(field)).getOrElse("")
    if (urlEncoded) {
      try {
        pField = URLDecoder.decode(pField, "UTF-8")
      }
      catch {
        case e: Exception => {
          pField = "Cannot Url Decode"
        }
      }
    }
    val pFields = pField.split(delimiter)
    val size = if (pFields.size > oFields.size) {
      oFields.size
    } else {
      pFields.size
    }
    for (i <- 0 to size - 1) {
      tuple.add(pFields(i))

    }
    var i = pFields.size
    while (i < oFields.size()) {
      tuple.add("")
      i += 1
    }
    call.getOutputCollector.add(tuple)
  }), Fields.ALL)

  setTails(assembly)
}

// similar to FieldExpander but only adds the fields you want to the tuple instead of all the fields
class FieldExpanderWithSubset(var assembly: Pipe, field: String, expandFields: Fields, delimiter: String, oFields: Fields, urlEncoded: Boolean = false) extends SubAssembly {
  val oFieldsArr: Array[String] = grvcascading.fieldsToArray(oFields)

  assembly = new Each(assembly, new GrvFunc(oFieldsArr: _*)((flow, call) => {
    flow.ctr("Field Expander: " + field)
    var pField = Option(call.getArguments.getString(field)).getOrElse("")
    if (urlEncoded) {
      try {
        pField = URLDecoder.decode(pField, "UTF-8")
      }
      catch {
        case e: Exception => {
          pField = "Cannot Url Decode"
        }
      }
    }
    val pFields = pField.split(delimiter)
    val size = if (pFields.size > oFields.size) {
      oFields.size
    } else {
      pFields.size
    }

    val out = Array.fill[String](oFieldsArr.size)("")
    for (i <- 0 to size - 1) {
      val pos = oFieldsArr.indexOf(expandFields.get(i).toString)
      if (pos > -1) {
        out(pos) = pFields(i)
      }
    }
    val tpl = new Tuple()
    tpl.addAll(out: _*)
    call.getOutputCollector.add(tpl)
  }), Fields.ALL)

  setTails(assembly)
}

class Coalesce(var assembly: Pipe, outputSelector: Fields, coalesceRules: Map[String, Vector[String]]) extends SubAssembly {
  assembly = new Each(assembly, new GrvFunc2(outputSelector)((flow, call) => {
    val out = call.getArguments.selectEntryCopy(outputSelector)
    coalesceRules.foreach { case (name: String, fieldNames: Vector[String]) => {
      out.setRaw(name, call.coalesceUnTyped(fieldNames:_*)())
    }}
    call.getOutputCollector.add(out)
  }))
  setTails(assembly)
}

case class FlowResult(name: String, flow: Flow[_])

abstract class CascadingJob extends Serializable {

  type FlowType = Flow[_]
  type FlowMaker = () => FlowType

  private val flowDefs = mutable.Map[String, FlowMaker]()

  def flow(name: String)(maker: => Flow[_]): Unit = {
    flowDefs += name -> (() => maker)
  }

  def run(): Seq[FlowResult] = {
    flowDefs.par.map {
      case (name, flowMaker) =>
        runFlow(name, flowMaker)
    }.toSeq.seq
  }

  def run(name: String): FlowResult = {
    runFlow(name, flowDefs(name))
  }

  private def runFlow(name: String, maker: FlowMaker): FlowResult = {
    val flow = maker()
    flow.complete()
    FlowResult(name, flow)
  }

}
