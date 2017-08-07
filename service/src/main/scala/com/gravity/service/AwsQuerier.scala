package com.gravity.service

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkClient
import com.amazonaws.services.elasticbeanstalk.model.{DescribeEnvironmentResourcesRequest, DescribeEnvironmentsRequest}
import com.gravity.utilities.GrvConcurrentMap
import com.gravity.utilities.components.FailureResult

import scalaz.ValidationNel
import scalaz.syntax.validation._

/*
*     __         __
*  /"  "\     /"  "\
* (  (\  )___(  /)  )
*  \               /
*  /               \
* /    () ___ ()    \  erik 8/15/16
* |      (   )      |
*  \      \_/      /
*    \...__!__.../
*/

object AwsQuerier {

  import scala.collection.JavaConversions._
  private val countMap = new GrvConcurrentMap[String,  ValidationNel[FailureResult, Int]]()

  private val creds = new BasicAWSCredentials("AKIAICAN5OXELAS4B53Q", "fDFTMChuV1NF+O7SDfPDTycebP5TzUv9SyPAaP5i")
  private val aasClient = new AmazonAutoScalingClient(creds)
  private val client = new AWSElasticBeanstalkClient(creds)
  client.setRegion(Region.getRegion(Regions.US_WEST_2))
  aasClient.setRegion(Region.getRegion(Regions.US_WEST_2))

  def getRoleServerCount(roleName: String): ValidationNel[FailureResult, Int] = {
    countMap.getOrElseUpdate(roleName, queryRoleServerCount(roleName))
  }

  private def queryRoleServerCount(roleName: String): ValidationNel[FailureResult, Int] = {
    try {
      val req = new DescribeEnvironmentsRequest()
      val awsEnvironmentName = roleName.toLowerCase.replace('_', '-')
      req.setApplicationName("interests")
      val envs = client.describeEnvironments(req).getEnvironments.toList.sortBy(_.getEnvironmentName)
      envs.find(_.getEnvironmentName == awsEnvironmentName) match {
        case Some(env) =>
          val env = envs.head
          val resReq = new DescribeEnvironmentResourcesRequest()
          resReq.setEnvironmentName(env.getEnvironmentName)
          val envResources = client.describeEnvironmentResources(resReq)
          val asgs = envResources.getEnvironmentResources.getAutoScalingGroups.toList
          if (asgs.length != 1) {
            FailureResult("Found " + asgs.size + " auto scaling groups for " + roleName + " and don't know what to do with that.").failureNel
          }
          else {
            val asg = asgs.head
            val asgReq = new DescribeAutoScalingGroupsRequest()
            asgReq.setAutoScalingGroupNames(List(asg.getName))
            val asgs2 = aasClient.describeAutoScalingGroups(asgReq).getAutoScalingGroups.toList
            if (asgs2.size != 1) {
              FailureResult("Found " + asgs2.size + " auto scaling groups for " + roleName + " and don't know what to do with that.").failureNel
            }
            else {
              asgs2.head.getMaxSize.toInt.successNel
            }
          }
        case None =>
          FailureResult("Could not find aws environment for role " + roleName).failureNel
      }
    }
    catch {
      case e: Exception => FailureResult("Exception querying role server count", e).failureNel
    }
  }
}




object AwsQuerierTesting extends App {
  println(AwsQuerier.getRoleServerCount("API_ROLE"))
}