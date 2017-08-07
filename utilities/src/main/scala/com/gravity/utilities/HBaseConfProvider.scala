package com.gravity.interests.jobs

import java.io._
import java.net.{URL, URLClassLoader}
import java.nio.file.Files
import java.util.Date

import com.gravity.utilities._
import com.gravity.utilities.grvenum.GrvEnum
import com.gravity.utilities.grvstrings._
import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


package object hbase {

  object PropertyNames {
    val defaultEnvironment = "hadoop.environment.default"

    // hdfs
    val defaultClusterBase = "hadoop.hdfs.cluster.default."
    val fsDefaultName = "hadoop.hdfs.filesystem"
    val zookeeperHAQuorum = "hadoop.hdfs.ha.zookeeper.quorum"
    val nameServices = "hadoop.hdfs.dfs.nameservices"
    val proxyRoot = "hadoop.hdfs.dfs.client.failover.proxy.provider."
    val autoFailoverEnabledRoot = "hadoop.hdfs.dfs.ha.automatic-failover.enabled."
    val nameNodesRoot = "hadoop.hdfs.dfs.ha.namenodes."
    val rpcRoot = "hadoop.hdfs.dfs.namenode.rpc-address."

    val zookeeperHBaseQuorum = "hbase.zookeeper.quorum"
    val zookeeperClientPort = "hadoop.zookeeper.clientport"

    val sparkEventLogDir = "spark.eventLog.dir"
    val sparkYarnHistoryServerAddress = "spark.yarn.historyServer.address"
  }

  object HBaseConfProvider {
 import com.gravity.logging.Logging._
    private var currentEnvironment = {
      if (Settings.isAws) HBaseEnvironments.AWS
      else HBaseEnvironments.get(Settings2.getProperty(PropertyNames.defaultEnvironment).getOrElse(HBaseEnvironments.DEVELOPMENT.name)).getOrElse(HBaseEnvironments.DEVELOPMENT)
    }

    info("HBase default environment is set to " + currentEnvironment)
    private val defaultDevelopmentCluster = HBaseClusters.get(Settings2.getProperty(PropertyNames.defaultClusterBase + HBaseEnvironments.DEVELOPMENT.name).getOrElse(HBaseClusters.MAIN.name)).getOrElse(HBaseClusters.MAIN)
    private val defaultAwsCluster = HBaseClusters.get(Settings2.getProperty(PropertyNames.defaultClusterBase + HBaseEnvironments.AWS.name).getOrElse(HBaseClusters.MAIN.name)).getOrElse(HBaseClusters.MAIN)

    def isUnitTest: Boolean = currentEnvironment == HBaseEnvironments.TEST
    def isAws: Boolean = currentEnvironment == HBaseEnvironments.AWS

    def setAws(): GrvHBaseConf = {
      if(currentEnvironment != HBaseEnvironments.AWS) {
        info("Setting current hbase environment to Aws")
        currentEnvironment = HBaseEnvironments.AWS
      }
      awsConf
    }

    def setUnitTest(): GrvHBaseConf = {
      if(currentEnvironment != HBaseEnvironments.TEST) {
        info("Setting current hbase environment to unit test")
        currentEnvironment = HBaseEnvironments.TEST
      }
      testConf
    }

    private def getPropertyName(environment: HBaseEnvironments.Type, clusterName: HBaseClusters.Type, propertyName: String) : String = {
        environment.name + "." + clusterName +  "." + propertyName
    }

    private val environmentConf: Configuration = {
      val conf = new Configuration(true)

      try {
        Option(System.getenv("HADOOP_CONF_DIR")).filter(_.nonEmpty).foreach(confDir => {
          info(s"HADOOP_CONF_DIR=$confDir, will initialized hadoop configuration using config files in this path")
          val configFiles = confDir.split(':').map(dir => new File(dir)).filter(_.exists()).flatMap(_.listFiles.filter(_.getName.endsWith(".xml")))
          configFiles.foreach(f => {
            info(s"Adding configuration resource: $f")
            conf.addResource(f.toURI.toURL)
          })
        })
      } catch {
        case ex: Exception => warn(ex, s"Unable to load environment configuration from HADOOP_CONF_DIR!")
      }

      conf
    }

    private def getNameServicePropertyName(environment: HBaseEnvironments.Type, clusterName: HBaseClusters.Type, root: String, nameService: String): String = {
      getPropertyName(environment, clusterName, root + nameService)
    }

    private def getNameNodePropertyName(environment: HBaseEnvironments.Type, clusterName: HBaseClusters.Type, root: String, nameService: String, nameNode: String): String = {
      getPropertyName(environment, clusterName, s"$root$nameService.$nameNode")
    }

    private def getConfFor(environment: HBaseEnvironments.Type, cluster: HBaseClusters.Type): Configuration = {

      if(environment == HBaseEnvironments.TEST) {
        info("Initializing HBase Client Config for test environment in cluster " + cluster.name)
        val conf: Configuration = HBaseConfiguration.create()
        conf.set("fs.defaultFS", s"file://${Settings.tmpDir}/hdfstmp/")
        conf.set("mapred.job.tracker","local")

        // spark props for testing
        conf.set("spark.master", "local[*]")
	      conf.set("spark.app.name", "test")
        conf.set("spark.app.id", new Date().toString + math.floor(math.random * 10E4).toLong.toString)
	      conf.set("spark.ui.enabled", "false")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", "com.gravity.interests.jobs.intelligence.helpers.grvspark.GrvSparkKryoRegistrator")
        conf.set("spark.eventLog.enabled", "false")

        conf
      }
      else {
        val failedBuffer: mutable.Buffer[String] = scala.collection.mutable.Buffer[String]()

        def getProperty(name: String): String = {
          Settings2.getProperty(name).getOrElse {
            failedBuffer += ("Failed to get HBase Client property: " + name)
            emptyString
          }
        }

        // load default configuration using classpath resources
        val conf = new Configuration(environmentConf)

        val nameService: String = getProperty(getPropertyName(environment, cluster, PropertyNames.nameServices))
        val fsDefaultName: String = getProperty(getPropertyName(environment, cluster, PropertyNames.fsDefaultName))
        val zookeeperQuorum: String = getProperty(getPropertyName(environment, cluster, PropertyNames.zookeeperHBaseQuorum))
        val zookeeperHAQuorum: String = getProperty(getPropertyName(environment, cluster, PropertyNames.zookeeperHAQuorum))
        val zookeeperClientPort: String = getProperty(getPropertyName(environment, cluster, PropertyNames.zookeeperClientPort))
        val proxy: String = getProperty(getNameServicePropertyName(environment, cluster, PropertyNames.proxyRoot, nameService))
        val autoFailover: String = getProperty(getNameServicePropertyName(environment, cluster, PropertyNames.autoFailoverEnabledRoot, nameService))

        val sparkEventLogDir: String = getProperty(getPropertyName(environment, cluster, PropertyNames.sparkEventLogDir))
        val sparkYarnHistoryServiceAddress: String = getProperty(getPropertyName(environment, cluster, PropertyNames.sparkYarnHistoryServerAddress))

        val nameNodes: String = getProperty(getNameServicePropertyName(environment, cluster, PropertyNames.nameNodesRoot, nameService))

        val (nameNode1, nameNode2) = {
          val bits = nameNodes.splitBetter(",", 2)
          bits.lift(0).getOrElse(emptyString) -> bits.lift(1).getOrElse(emptyString)
        }

        val rpc1: String = getProperty(getNameNodePropertyName(environment, cluster, PropertyNames.rpcRoot, nameService, nameNode1))
        val rpc2: String = getProperty(getNameNodePropertyName(environment, cluster, PropertyNames.rpcRoot, nameService, nameNode2))

        val poolSize: String = Settings2.getPropertyOrDefault("hadoop.hbase.pool.size", "2")


        if (failedBuffer.isEmpty) {
          conf.set("fs.default.name", fsDefaultName)
          conf.set("dfs.nameservices", nameService)
          conf.set("dfs.ha.namenodes." + nameService, nameNodes)
          conf.set(s"dfs.namenode.rpc-address.$nameService.$nameNode1", rpc1)
          conf.set(s"dfs.namenode.rpc-address.$nameService.$nameNode2", rpc2)
          conf.set("dfs.client.failover.proxy.provider." + nameService, proxy)
          conf.set("dfs.ha.automatic-failover.enabled." + nameService, autoFailover)
          conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
          conf.set("ha.zookeeper.quorum", zookeeperHAQuorum)
          conf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
          info("Initializing HBase Client Config for " + environment.name + " environment in cluster " + cluster.name + ": fs.default.name = {0} :: dfs.nameservices = {1} :: dfs.ha.namenodes = {2} :: ha.zookeeper.quorum = {3} :: hbase.zookeeper.quorum = {4} :: hbase.zookeeper.property.clientPort = {5}",
            fsDefaultName, nameService, nameNodes, zookeeperHAQuorum, zookeeperQuorum, zookeeperClientPort)
          conf.set("hbase.client.ipc.pool.size", poolSize)
          conf.set("hbase.client.ipc.pool.type", "Reusable")
          conf.set(PropertyNames.sparkEventLogDir, sparkEventLogDir)
          conf.set(PropertyNames.sparkYarnHistoryServerAddress, sparkYarnHistoryServiceAddress)

          conf
        }
        else {
          throw new RuntimeException("Failed to initialize HBase Client Config due to: " + failedBuffer.mkString(" AND "))
        }
      }
    }

    private def getConfsFor(environment: HBaseEnvironments.Type) : Map[HBaseClusters.Type, Configuration] = {
      val clusterConfs = for {
        cluster <- HBaseClusters.all
      } yield {
        cluster -> getConfFor(environment, cluster)
      }

      clusterConfs.toMap
    }

    lazy val testConf: GrvHBaseConf = GrvHBaseConf(HBaseEnvironments.TEST, HBaseClusters.MAIN, getConfsFor(HBaseEnvironments.TEST))
    lazy val developmentConf: GrvHBaseConf = GrvHBaseConf(HBaseEnvironments.DEVELOPMENT, defaultDevelopmentCluster, getConfsFor(HBaseEnvironments.DEVELOPMENT))
    //lazy val productionConf: GrvHBaseConf = GrvHBaseConf(HBaseEnvironments.PRODUCTION, defaultProductionCluster, getConfsFor(HBaseEnvironments.PRODUCTION))
    lazy val awsConf: GrvHBaseConf = GrvHBaseConf(HBaseEnvironments.AWS, defaultAwsCluster, getConfsFor(HBaseEnvironments.AWS))

    def getConf: GrvHBaseConf = {
      currentEnvironment match {
        case HBaseEnvironments.TEST => testConf
        case HBaseEnvironments.DEVELOPMENT => developmentConf
        //case HBaseEnvironments.PRODUCTION => productionConf
        case HBaseEnvironments.AWS => awsConf
      }
    }

    def fs: FileSystem = {
      getConf.fs
    }
  }

  case class GrvHBaseConf(environment: HBaseEnvironments.Type, defaultCluster: HBaseClusters.Type, configurations: Map[HBaseClusters.Type, Configuration]) {
    val defaultConf: Configuration = configurations(defaultCluster)

    val oltpConf: Configuration = configurations(HBaseClusters.OLTP)

    lazy val fs: FileSystem = FileSystem.get(defaultConf)
  }

}


object HBaseEnvironments extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  override def defaultValue: Type = DEVELOPMENT

  val PRODUCTION: Type = Value(0, "production")
  val DEVELOPMENT: Type = Value(1, "development")
  val TEST: Type = Value(2, "test")
  val AWS: Type = Value(3, "aws")
}

//each environment can have multiple clusters
object HBaseClusters extends GrvEnum[Byte] {
  case class Type(i: Byte, n: String) extends ValueTypeBase(i, n)

  override def mkValue(id: Byte, name: String): Type = Type(id, name)

  override def defaultValue: Type = MAIN

  val MAIN: Type = Value(0, "main")
  val DLUG: Type = Value(1, "dlug")
  val OLTP: Type = Value(2, "oltp")

  val all: Seq[Type] = Seq(MAIN, DLUG, OLTP)
}