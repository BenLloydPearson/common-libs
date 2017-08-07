package com.gravity.service.remoteoperations

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RandomGroup
import akka.util.Timeout
import com.gravity.service._
import com.gravity.utilities._
import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvfields.FieldConverter
import com.gravity.logging.Logging._
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scalaz.Scalaz._
import scalaz.{Failure, _}

object RemoteOperationsHelper { //separate object to avoid initializing remote ops stuff just by telling it it's not production
  var isProduction: Boolean = ServerRegistry.isProductionServer
  var isUnitTest = false

  private val replyConverters = new GrvConcurrentMap[String, FieldConverter[_]]()

  def registerReplyConverter(converter: FieldConverter[_]) {
    replyConverters.update(converter.getCategoryName, converter)
  }

  def getReplyConverter(categoryName: String): Option[FieldConverter[_]] = replyConverters.get(categoryName)
}

case class SendWrapper[R](payload: R, payloadTypeName: String, routeIdOpt: Option[Long], reservedServersOpt: Option[Int], fieldConverterOpt: Option[FieldConverter[R]])
case class SendWrapperFunc[R](payload: R, payloadTypeName: String, routeFunc: (String, R) => Option[Seq[Long]], fieldConverter: FieldConverter[R])

class RemoteOperationsDoer(var roleProvider : RoleProvider, var endpoints: RemoteEndpoints) extends Actor {
 import com.gravity.logging.Logging._

  override def receive: Receive = {
    case rp: RoleProvider => roleProvider = rp
    case ep: RemoteEndpoints => endpoints = ep
    case sw @ SendWrapper(payload: Any, payloadTypeName: String, routeIdOpt: Option[Long],  reservedServersOpt: Option[Int], fieldConverterOpt: Option[FieldConverter[_]]) =>
      sendWrappers(createWrappers(payload, payloadTypeName, routeIdOpt, reservedServersOpt, fieldConverterOpt))
    case swf @ SendWrapperFunc(payload: Any, payloadTypeName: String, routeIdFunc: ((String, Any) => Option[Seq[Long]]), fieldConverter: FieldConverter[_]) =>
      sendWrappers(createWrappers(payload, payloadTypeName, routeIdFunc, fieldConverter))

  }

  private def createWrappers(payload: Any, payloadTypeName: String, routeIdOpt: Option[Long], reservedServersOpt: Option[Int] = None, fieldConverterOpt: Option[FieldConverter[Any]] = None)
                               : Validation[FailureResult, Seq[MessageWrapper]] = {
    val infoOpt = roleProvider.typeMapper.getInfoFor(payloadTypeName)

    infoOpt match {
      case Some(info) =>
        fieldConverterOpt match {
          case None =>
            val bos = new ByteArrayOutputStream()
            val oos = new ObjectOutputStream(bos)
            try {
              oos.writeObject(payload)
              bos.close()
              oos.close()
              val payloadBytes = bos.toByteArray
              val wrappers = for (role <- info.roles)
                yield MessageWrapper(payloadBytes, payloadTypeName, 1, role, routeIdOpt, reservedServersOpt = reservedServersOpt)
              wrappers.success
            }
            catch {
              case e: Exception =>
                critical(e, "Could not serialize object for remoting: " + payload)
                FailureResult("Could not serialize object for remoting " + ScalaMagic.formatException(e)).failure
            }
          case Some(converter) =>
            try {
              val payloadBytes = converter.toBytes(payload)
              val wrappers = for (role <- info.roles)
                yield MessageWrapper(payloadBytes, payloadTypeName, 2, role, routeIdOpt, fieldCategory = converter.getCategoryName, reservedServersOpt = reservedServersOpt)
              wrappers.success
            }
            catch {
              case e: Exception =>
                val message = "Could not serialize object for remoting with field serializer: " + payload + ": " + ScalaMagic.formatException(e)
                critical(message)
                FailureResult(message).failure
            }
        }
      case None =>
        FailureResult("No type info found for " + payloadTypeName + ". Define it in TypeMapping.scala").failure
    }
  }

  private def createWrappers(payload: Any, payloadTypeName: String, routeIdFunc: (String, Any) => Option[Seq[Long]], fieldConverter: FieldConverter[Any])
  : Validation[FailureResult, Seq[MessageWrapper]] = {
    val infoOpt = roleProvider.typeMapper.getInfoFor(payloadTypeName)

    infoOpt match {
      case Some(info) =>
        try {
          val payloadBytes = fieldConverter.toBytes(payload)
          val wrapperBuf = ArrayBuffer[MessageWrapper]()
          for (role <- info.roles) {
            routeIdFunc(role, payload) match {
              case Some(ids) =>
                if(ids.isEmpty) { //function returned something, but with no numbers = send to random
                  wrapperBuf += MessageWrapper(payloadBytes, payloadTypeName, 2, role, None, fieldCategory = fieldConverter.getCategoryName, reservedServersOpt = None)
                }
                else for(id <- ids) {
                  wrapperBuf += MessageWrapper(payloadBytes, payloadTypeName, 2, role, Some(id), fieldCategory = fieldConverter.getCategoryName, reservedServersOpt = None)
                }
              case None => //don't send to this role
            }
          }
          wrapperBuf.success
        }
        catch {
          case e: Exception =>
            val message = "Could not serialize object for remoting with field serializer: " + payload + ": " + ScalaMagic.formatException(e)
            critical(message)
            FailureResult(message).failure
        }
      case None =>
        FailureResult("No type info found for " + payloadTypeName + ". Define it in TypeMapping.scala").failure
    }
  }


  private def sendWrappers(wrappersValidation : Validation[FailureResult, Seq[MessageWrapper]]) : Seq[Validation[FailureResult, MessageWrapper]] = {
    wrappersValidation match {
      case Success(wrappers) =>
        for (wrapper <- wrappers) yield
          endpoints.getEndpointsFor(wrapper.sentToRole).send(wrapper)
      case Failure(why) =>
        warn(why.toString)
        Seq(why.failure)
    }
  }
}

object RemoteOperationsClient {
 import com.gravity.logging.Logging._
  import Counters._
  val haltedRedirectsCounter: PerSecondCounter = getOrMakePerSecondCounter("Remote Ops Client", "Remote Ops Client Redirect Halts")
  val mailboxCounter: AverageCounter = getOrMakeAverageCounter("Remote Ops Client", "Mailbox Size : Send")
  val pinnedDispatcherName = "RemoteOperationsClient-Pinned"
  val mailboxCapacity = 500
  val conf: Config = ConfigFactory.load(ConfigFactory.parseString(
  pinnedDispatcherName + """
    {
      type = PinnedDispatcher
      mailbox-type = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType"
      mailbox-capacity = """ + mailboxCapacity + """
      mailbox-push-timeout-time = "0ms"
      executor = "thread-pool-executor"
    }
    akka {
      daemonic = on
    }
      """))

  implicit val system: ActorSystem = ActorSystem("RemoteOperationsClient", conf)

  implicit def durationToTimeout(value: scala.concurrent.duration.Duration) : akka.util.Timeout = {
    new Timeout(value.toMillis, TimeUnit.MILLISECONDS)
  }

  def isProduction: Boolean = RemoteOperationsHelper.isProduction
  def isUnitTest: Boolean = RemoteOperationsHelper.isUnitTest

  def clientInstance: RemoteOperationsClient = {
    if (isProduction || Settings2.getBooleanOrDefault("operations.enable-prod-remoting", default = false)) {
      //info("Using Production Remote Operations Client")
      ProductionRemoteOperationsClient //prod zookeeper
    }
    else if (isUnitTest) {
      //info("Using Test Remote Operations Client")
      TestRemoteOperationsClient //in memory registry
    }
    else {
      //info("Using Development Remote Operations Client")
      DevelopmentRemoteOperationsClient //dev zookeeper
    }
  }

}

trait SplittablePayload[T] {
  def split(into: Int): Array[Option[T]]

  def bucketIndexFor(id: Long, into: Int): Int =  {
    scala.math.abs((id % into).toInt)
  }

  def size : Int
}

trait MergeableResponse[T] {
  def merge(withThese: Seq[T]) : T
  def size : Int
}

case class SplitRequestResponse[T <: MergeableResponse[T]](responseObjectOption: Option[T], failures: List[FailureResult])

class RemoteOperationsClient(var roleProvider : RoleProvider) {
 import com.gravity.logging.Logging._
  import RemoteOperationsClient.system
  private var endpoints = new RemoteEndpoints(roleProvider)

  private val doers = {
    for(i <- 0 until 8) yield { //we usually run with eight CPUs
      val actor = system.actorOf(Props(new RemoteOperationsDoer(roleProvider, endpoints)))
      actor
    }
  }

  private val doersGroup : ActorRef = system.actorOf(RandomGroup(doers.map(_.path.toString)).props())

  /**
   * Force connections to remote hosts in init block on server startup
   * 
   * @param roleName The role to connect to
   */
  def init(roleName: String) {
    endpoints.getEndpointsFor(roleName)
  }

  def setRoleProvider(newRoleProvider: RoleProvider) {
    //this is inherently not concurrency safe, but in practice it's only called at startup and without active traffic
    this.roleProvider = newRoleProvider
    endpoints = new RemoteEndpoints(roleProvider)
    doers.foreach( _ ! roleProvider)
    doers.foreach( _ ! endpoints)
  }

  def stopActors() {
    endpoints.disconnectAll()
  }

  def belongsHere(message : MessageWrapper, routeHint: Long) : Boolean = {
    endpoints.belongsHere(message, routeHint)
  }

  def pullActor(badActor : RemoteEndpoint) {
    endpoints.disconnect(badActor)
  }

  def sendSplit[R <: SplittablePayload[R]](payload: R, fieldConverter: FieldConverter[R])
                                          (implicit m: Manifest[R]) : Seq[Validation[FailureResult, MessageWrapper]] =  {
    val infoOpt = roleProvider.typeMapper.getInfoFor[R]

    val returns = infoOpt match {
       case Some(info) =>
         val results =
           for (role <- info.roles) yield {
             val roleEndpoints = endpoints.getEndpointsFor(role)
             val activeEndpoints = roleEndpoints.getAllActiveEndpoints
             try {
               val splitPayloads = payload.split(activeEndpoints.size)
               //right now if there's a disconnect while this is executing, the message for the disconnected server will end up in a random place. but really
               //if that happens the send is invalid anyway, since the active list changed.
               //helpful this isn't actually used anywhere except a test at the moment, so the solution/consideration can be made when we have a use case
               val messageWrapperOptions = splitPayloads.map{payloadOpt => {
                 payloadOpt map { payload => MessageWrapper(fieldConverter.toBytes(payload), m.runtimeClass.getCanonicalName, 2, role, None, fieldCategory = fieldConverter.getCategoryName) }
               }}
               val sendResults = for (i <- messageWrapperOptions.indices) yield {
                 messageWrapperOptions(i).map(activeEndpoints(i).sendOneWay)
               }
               sendResults.flatten
             }
             catch {
               case e: Exception =>
                 warn("Exception sending multi: " + ScalaMagic.formatException(e))
                 Seq(Failure(FailureResult("Exception sending multi", e)))
             }
           }
         results.flatten
       case None =>
         val typeName = m.runtimeClass.getCanonicalName
         val message = "No type info found for " + typeName + ". Define it in TypeMapping.scala"
         Seq(FailureResult(message).failure)
     }

    returns
  }

  def requestResponseSplit[R <: SplittablePayload[R], T <: MergeableResponse[T]](payload: R, timeOut: scala.concurrent.duration.Duration, requestConverter: FieldConverter[R])
                                                                                (implicit m: Manifest[R], mt: Manifest[T]) : SplitRequestResponse[T] = {
    val infoOpt = roleProvider.typeMapper.getInfoFor[R]
    val checkResponseSizes = true //turn this off when I'm satisfied the "wrong answer" bug is well and truly gone

    infoOpt match {
      case Some(info) =>
        val roleName = info.roles.head
        if (info.roles.size > 1) warn("Splittable type " + info.name + " defines more that one role, which is unsupported. Using " + roleName)
        val successes = new ArrayBuffer[T]()
        val failures = new ArrayBuffer[FailureResult]

        val roleEndpoints = endpoints.getEndpointsFor(roleName)
        val activeEndpoints = roleEndpoints.getAllActiveEndpoints
        if (activeEndpoints.isEmpty) {
          return SplitRequestResponse(None, List(FailureResult("There are no active servers in " + roleName)))
        }
        try {
          val splitPayloads = payload.split(activeEndpoints.size)
          val messageWrapperOptions = splitPayloads.map { payloadOpt => {
            payloadOpt map { payload =>
              val wrapper = MessageWrapper(requestConverter.toBytes(payload), m.runtimeClass.getCanonicalName, 2, roleName, None, fieldCategory = requestConverter.getCategoryName)
              wrapper.requestItemCount = payload.size
              wrapper
            }
          }
          }
          for (i <- messageWrapperOptions.indices) {
            messageWrapperOptions(i).foreach(activeEndpoints(i).sendToForReplyAsync[T](_, timeOut))
          }

          roleEndpoints.asyncRequestPool(timeOut.toMillis.toInt, cancelIfTimedOut = true) {
            for (i <- messageWrapperOptions.indices) {
              messageWrapperOptions(i).foreach { wrapper => {
                if (wrapper.replyCdl.await(timeOut.toMillis, TimeUnit.MILLISECONDS)) {
                  wrapper.replyOption match {
                    case Some(resultBytes) =>
                      val validation = RemoteEndpoint.deserialize[T](resultBytes, wrapper.sentToHost)
                      if (checkResponseSizes) {
                        validation.map(mergable => {
                          if (mergable.size != wrapper.requestItemCount) {
                            warn("Requested " + wrapper.requestItemCount + " items but got " + mergable.size + " items back from " + wrapper.sentToHost)
                          }
                        })
                      }
                      validation match {
                        case Success(item) => successes.append(item)
                        case Failure(fails) => failures.appendAll(fails.list)
                      }
                    case None =>
                      wrapper.replyFailureOption match {
                        case Some(failure) => failures.append(failure)
                        case None => failures.append(FailureResult("Neither reply nor error found after timeout"))
                      }
                  }
                }
                else {
                  failures.append(FailureResult("Time out in split request for " + wrapper.requestItemCount + " items to " + wrapper.sentToHost))
                }
              }
              }
            }
            val mergedSuccesses = if (successes.nonEmpty) Some(successes.head.merge(successes.tail)) else None

            SplitRequestResponse(mergedSuccesses, failures.toList).success
          } match {
            case Success(splitRequestResponse) => splitRequestResponse
            case Failure(fails) => SplitRequestResponse[T](None, fails.toList)
          }
        }
        catch {
          case e: Exception =>
            warn("Exception sending multi: " + ScalaMagic.formatException(e))
            SplitRequestResponse[T](None, List(FailureResult("Exception sending multi", e)))
        }


      case None =>
        val message = "No type info found for " + m.runtimeClass.getCanonicalName + ". Define it in TypeMapping.scala"
        warn(message)
        SplitRequestResponse[T](None, List(FailureResult(message)))
    }
  }

  private def createWrappers[R](payload: R, routeIdOpt: Option[Long], reservedServersOpt: Option[Int] = None, fieldConverterOpt: Option[FieldConverter[R]] = None)
                               (implicit m: Manifest[R]) : Validation[FailureResult, Seq[MessageWrapper]] = {
    val infoOpt = roleProvider.typeMapper.getInfoFor[R]

    infoOpt match {
      case Some(info) =>
        fieldConverterOpt match {
          case None =>
            val bos = new ByteArrayOutputStream()
            val oos = new ObjectOutputStream(bos)
            try {
              oos.writeObject(payload)
              bos.close()
              oos.close()
              val payloadBytes = bos.toByteArray
              val wrappers = for (role <- info.roles)
              yield MessageWrapper(payloadBytes, m.runtimeClass.getCanonicalName, 1, role, routeIdOpt, reservedServersOpt = reservedServersOpt)
              wrappers.success
            }
            catch {
              case e: Exception =>
                critical(e, "Could not serialize object for remoting: " + payload)
                FailureResult("Could not serialize object for remoting " + ScalaMagic.formatException(e)).failure
            }
          case Some(converter) =>
            try {
              val payloadBytes = converter.toBytes(payload)
              val wrappers = for (role <- info.roles)
              yield MessageWrapper(payloadBytes, m.runtimeClass.getCanonicalName, 2, role, routeIdOpt, fieldCategory = converter.getCategoryName, reservedServersOpt = reservedServersOpt)
              wrappers.success
            }
            catch {
              case e: Exception =>
                val message = "Could not serialize object for remoting with field serializer: " + payload + ": " + ScalaMagic.formatException(e)
                critical(message)
                FailureResult(message).failure
            }
        }
      case None =>
        val typeName = m.runtimeClass.getCanonicalName
        FailureResult("No type info found for " + typeName + ". Define it in TypeMapping.scala").failure
    }
  }

  def broadcast[R](payload: R)(implicit m: Manifest[R]): Seq[Validation[FailureResult, MessageWrapper]] = {
    createWrappers(payload, None) match {
      case Success(wrappers) =>
        (for (wrapper <- wrappers) yield
          endpoints.getEndpointsFor(wrapper.sentToRole).getAllActiveEndpoints.map(endpoint => {
            wrapper.sendCount = 0 //the previous one was already sent across the wire, have to reset to avoid confusing things here
            endpoint.sendOneWay(wrapper)
          })).flatten
      case Failure(why) =>
        warn(why.toString)
        Seq(why.failure)
    }
  }

  def send[R](payload: R, fieldConverterOpt: Option[FieldConverter[R]] = None)
             (implicit m: Manifest[R]): Unit = {

    doersGroup ! SendWrapper(payload, m.runtimeClass.getCanonicalName, None, None, fieldConverterOpt)
  }

  def send[R](payload: R, routeId: Long, fieldConverterOpt: Option[FieldConverter[R]])
             (implicit m: Manifest[R]): Unit = {
    doersGroup ! SendWrapper(payload, m.runtimeClass.getCanonicalName, Some(routeId), None, fieldConverterOpt)
  }

  def send[R](payload: R, routeFunc: (String, R) => Option[Seq[Long]], fieldConverter: FieldConverter[R])
             (implicit m: Manifest[R]): Unit = {
    doersGroup ! SendWrapperFunc(payload, m.runtimeClass.getCanonicalName, routeFunc, fieldConverter)
  }

  def sendWithWrapperCreationValidation[R: Manifest](payload: R) : Seq[Validation[FailureResult, MessageWrapper]] = {
   sendWrappers(createWrappers(payload, None, None))
  }

  private def sendWrappers(wrappersValidation : Validation[FailureResult, Seq[MessageWrapper]]) : Seq[Validation[FailureResult, MessageWrapper]] = {
    wrappersValidation match {
      case Success(wrappers) =>
        for (wrapper <- wrappers) yield
          endpoints.getEndpointsFor(wrapper.sentToRole).send(wrapper)
      case Failure(why) =>
        warn(why.toString)
        Seq(why.failure)
    }
  }

  def requestResponse[R,T](payload: R, timeOut: scala.concurrent.duration.Duration, fieldConverterOpt: Option[FieldConverter[R]] = None)
                          (implicit mr: Manifest[R], mt: Manifest[T])
  : ValidationNel[FailureResult, T] = {
    requestResponse[T](createWrappers(payload, None, fieldConverterOpt = fieldConverterOpt), timeOut)
  }

  def requestResponse[R, T](payload: R, routeId: Long, timeOut: scala.concurrent.duration.Duration, fieldConverterOpt: Option[FieldConverter[R]])
                           (implicit mr: Manifest[R], mt: Manifest[T]) : ValidationNel[FailureResult, T] = {
    requestResponse[T](createWrappers(payload, Some(routeId), fieldConverterOpt = fieldConverterOpt), timeOut)
  }

  def requestResponse[R, T](payload: R, routeId: Long, reservedServers: Int, timeOut: scala.concurrent.duration.Duration, fieldConverterOpt: Option[FieldConverter[R]])(implicit mr: Manifest[R], mt: Manifest[T]) : ValidationNel[FailureResult, T] = {
    requestResponse[T](createWrappers(payload, Some(routeId), Some(reservedServers), fieldConverterOpt = fieldConverterOpt), timeOut)

  }

  private def requestResponse[T](wrappersValidation : Validation[FailureResult, Seq[MessageWrapper]], timeOut: scala.concurrent.duration.Duration)
                                (implicit mt: Manifest[T]) : ValidationNel[FailureResult, T] = {
    wrappersValidation match {
      case Success(wrappers) =>
        val wrapper = wrappers.head
        endpoints.getEndpointsFor(wrapper.sentToRole).request[T](wrapper, timeOut)
      case Failure(why) =>
        warn(why.toString)
        why.failureNel
    }
  }

  def routeGoesTo(routeId: Long, roleName: String): String = {
    endpoints.getModdedActiveEndpointFor(roleName, routeId, None) match {
      case Some(service) => service.hostAddress + ":" + service.port
      case None => "nowhere"
    }
  }

}

object ProductionRemoteOperationsClient extends RemoteOperationsClient(AwsZooWatcherByRole)
object DevelopmentRemoteOperationsClient extends RemoteOperationsClient(DevelopmentZooWatcherByRole)

object TestRemoteOperationsClient extends RemoteOperationsClient(TestRoleProvider) {
  lazy val currentPort: AtomicInteger = new AtomicInteger(50000 + new Random().nextInt(10000))
  def getPort: Int = {
    currentPort.getAndIncrement()
  }


  def createServer(roleName: String, components: Seq[ServerComponent[_ <: ComponentActor]]): RemoteOperationsServer = {
    val tcpPort = getPort
    val server = new RemoteOperationsServer(tcpPort, components = components)
    //sets the last heard from into the future so the age check doesn't bump against it
    //val roleData = RoleData(ServerRegistry.hostName, Array(roleName), new DateTime().plusYears(1), Settings.APPLICATION_BUILD_NUMBER, ZooCommon.instanceName + ":" + tcpPort, servicePorts = Seq.empty[Int], portMap = Map.empty, newPortMap = Map(roleName -> Array(tcpPort)))
    val roleData = RoleData(roleName = roleName, lastHeardFrom = new DateTime().plusYears(1), remoteOperationsPort = tcpPort, serverIndex = 0)
    println(roleData)
    server.start()
    TestRoleProvider.addData(roleData)
    server
  }

  def testServer(server: RemoteOperationsServer, roleName: String): Unit = {
    //sets the last heard from into the future so the age check doesn't bump against it
    val tcpPort = server.tcpPort
    //val roleData = RoleData(ServerRegistry.hostName, Array(roleName), new DateTime().plusYears(1), Settings.APPLICATION_BUILD_NUMBER, ZooCommon.instanceName + ":" + tcpPort, servicePorts = Seq.empty[Int], portMap = Map.empty, newPortMap = Map(roleName -> Array(tcpPort)))
    val roleData = RoleData(roleName = roleName, lastHeardFrom = new DateTime().plusYears(1), remoteOperationsPort = tcpPort, serverIndex = 0)
    println(roleData)
    server.start()
    TestRoleProvider.addData(roleData)
  }

  def stopServer(roleName: String, server: RemoteOperationsServer): Unit = {
    val roleData = RoleData(roleName = roleName, remoteOperationsPort = server.tcpPort, serverIndex = 0)
    //println(roleData)
    TestRoleProvider.removeData(roleData)
    server.stop()
  }
}

trait RemoteOperationsDispatcher {
  def sendRemoteMessage[R:Manifest](payload: R): Seq[Validation[FailureResult, MessageWrapper]]
}

trait ProductionRemoteOperationsDispatcher extends RemoteOperationsDispatcher {
  def sendRemoteMessage[R:Manifest](payload: R): Seq[Validation[FailureResult, MessageWrapper]] = ProductionRemoteOperationsClient.sendWithWrapperCreationValidation[R](payload)
}

trait TestRemoteOperationsDispatcher extends RemoteOperationsDispatcher {
  def sendRemoteMessage[R:Manifest](payload: R): Seq[Validation[FailureResult, MessageWrapper]] = TestRemoteOperationsClient.sendWithWrapperCreationValidation[R](payload)
}

object WhichServer extends App {
  val siteGuid = "361837bc83c4c2cba7b350dff56a564f"
  val numServers = 10
  val hash: Long = MurmurHash.hash64(siteGuid)

  println("site key id is " + hash)
  println(ProductionRemoteOperationsClient.routeGoesTo(hash, "INTEREST_INTELLIGENCE_OPERATIONS"))
}

//object AwsQuery extends App {
//import com.amazonaws.auth.BasicAWSCredentials
//import com.amazonaws.regions.{Region, Regions}
//import com.amazonaws.services.autoscaling.AmazonAutoScalingClient
//import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
//import com.amazonaws.services.elasticbeanstalk.AWSElasticBeanstalkClient
//import com.amazonaws.services.elasticbeanstalk.model.{DescribeEnvironmentResourcesRequest, DescribeEnvironmentsRequest}

//  import scala.collection.JavaConversions._
//
//  val creds = new BasicAWSCredentials("AKIAICAN5OXELAS4B53Q", "fDFTMChuV1NF+O7SDfPDTycebP5TzUv9SyPAaP5i")
//  val aasClient = new AmazonAutoScalingClient(creds)
//  val client = new AWSElasticBeanstalkClient(creds)
//  client.setRegion(Region.getRegion(Regions.US_WEST_2))
//  aasClient.setRegion(Region.getRegion(Regions.US_WEST_2))
//  val req = new DescribeEnvironmentsRequest()
//  req.setApplicationName("interests")
//
//
//  val envs = client.describeEnvironments(req).getEnvironments.toList.sortBy(_.getEnvironmentName)
//  envs.foreach{ env =>
//    //println(env.getEnvironmentName)
//    val resReq = new DescribeEnvironmentResourcesRequest()
//    resReq.setEnvironmentName(env.getEnvironmentName)
//    val envResources = client.describeEnvironmentResources(resReq)
//    val asgs = envResources.getEnvironmentResources.getAutoScalingGroups.toList
//    asgs.foreach{asg =>
//      //println(asg.getName)
//      val asgReq = new DescribeAutoScalingGroupsRequest()
//      asgReq.setAutoScalingGroupNames(List(asg.getName))
//      val asgs2 = aasClient.describeAutoScalingGroups(asgReq).getAutoScalingGroups.toList
//      asgs2.foreach{asg2 =>
//        println(env.getEnvironmentName + ": " + asg2.getDesiredCapacity)
//      }
//    }
//    //env.get
//  }
//}

