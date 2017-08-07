package com.gravity.interests.jobs.intelligence.hbase

import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ThreadPoolExecutor}

import com.gravity.interests.jobs.hbase.HBaseConfProvider
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.util.Threads

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * A thread pool configured in such a way as to match future versions of HBase.  To be passed into the HTable implementation.
 * This was suggested by Cloudera after our connection checkout woes.
 */
object ThreadPoolProvider {
  val conf = HBaseConfProvider.getConf.defaultConf
  val pool = {
    var maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
    var coreThreads = conf.getInt("hbase.hconnection.threads.core", 256);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    val keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
    val workQueue =
      new LinkedBlockingQueue[Runnable](maxThreads * 100)
    val tpe = new ThreadPoolExecutor(
      coreThreads,
      maxThreads,
      keepAliveTime,
      TimeUnit.SECONDS,
      workQueue,
      Threads.newDaemonThreadFactory(toString() + "-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    tpe
  }
}
