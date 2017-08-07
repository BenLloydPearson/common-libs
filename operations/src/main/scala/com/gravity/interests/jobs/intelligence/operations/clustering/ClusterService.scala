package com.gravity.interests.jobs.intelligence.operations.clustering

import com.gravity.interests.jobs.intelligence.hbase.ClusterScopeKey
import com.gravity.interests.jobs.intelligence.hbase.ScopedKeyConverters.ScopedKeyConverter
import java.util.Formatter

import com.gravity.interests.jobs.intelligence.hbase.ClusterTableConverters.ClusterScopeKeyConverter
import com.gravity.interests.jobs.hbase.HBaseConfProvider
import com.gravity.utilities.Settings

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
object ClusterService extends ClusterManager {

  def baseDir = if(HBaseConfProvider.isUnitTest) s"file://${Settings.tmpDir}/clusterunitbase/user/gravity/reports/clustering/" else "/user/gravity/reports/clustering/"


  def preparationDir = baseDir + "preparation/"

  def fromDirectoryPath(step:String, path:String) : ClusterScopeKey = {
    val key = path.split("/").last
    ClusterScopeKeyConverter.fromByteString(key)
  }

  def toDirectoryPath(step:String, key:ClusterScopeKey) = {

    val keyStr = ClusterScopeKeyConverter.toByteString(key)
    baseDir + step + "/" + keyStr + "/"
  }

}

object ClusterScopeService extends ClusterScopeManager
