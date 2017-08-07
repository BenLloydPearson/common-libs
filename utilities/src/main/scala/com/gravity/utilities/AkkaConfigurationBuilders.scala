package com.gravity.utilities

import com.typesafe.config.{Config, ConfigFactory}

object AkkaConfigurationBuilders {

  def boundedMailboxConf(dispatcherName: String,
                         mailboxCapacity: Int,
                         mailboxType: String = "com.gravity.utilities.grvakka.BoundedMeteredFILOMailboxType",
                         mailboxPushTimeoutTimeMs: Int = 0,
                         throughput: Int = 1,
                         numThreads: Int = 40): Config = ConfigFactory.load(ConfigFactory.parseString(
    s"""$dispatcherName
        |{
        |  type = Dispatcher
        |  mailbox-type = "$mailboxType"
        |  mailbox-capacity = $mailboxCapacity
        |  mailbox-push-timeout-time = "${mailboxPushTimeoutTimeMs}ms"
        |  executor = "fork-join-executor"
        |  throughput = $throughput
        |  fork-join-executor {
        |    parallelism-min = $numThreads
        |    parallelism-factor = 64
        |    parallelism-max = $numThreads
        |  }
        |}
        |akka {
        |  daemonic = on
        |  log-dead-letters-during-shutdown = off                                                  |
        |}
        |""".stripMargin))

  def dedupedUnboundedStableMailboxConf(dispatcherName: String,
                                        mailboxType: String = "com.gravity.utilities.DedupedUnboundedStableMailbox",
                                        throughput: Int = 1,
                                        numThreads: Int = 40): Config = ConfigFactory.load(ConfigFactory.parseString(
    s"""$dispatcherName
        |{
        |  type = "akka.dispatch.BalancingDispatcherConfigurator"
        |  mailbox-type = "$mailboxType"
        |  executor = "fork-join-executor"
        |  throughput = $throughput
        |  fork-join-executor {
        |    parallelism-min = $numThreads
        |    parallelism-factor = 64
        |    parallelism-max = $numThreads
        |  }
        |}
        |akka {
        |  daemonic = on
        |  log-dead-letters-during-shutdown = off
        |}
        |""".stripMargin))

  def dedupedUnboundedStablePriorityMailboxConf(dispatcherName: String,
                                                prioritizer: String,   // Fully-Qualified Class Path of Comparator[Message]
                                                mailboxType: String = "com.gravity.utilities.DedupedUnboundedStablePriorityMailbox",
                                                throughput: Int = 1,
                                                numThreads: Int = 40): Config = ConfigFactory.load(ConfigFactory.parseString(
    s"""$dispatcherName
        |{
        |  type = "akka.dispatch.BalancingDispatcherConfigurator"
        |  mailbox-type = "$mailboxType"
        |  executor = "fork-join-executor"
        |  prioritizer = "$prioritizer"
        |  throughput = $throughput
        |  fork-join-executor {
        |    parallelism-min = $numThreads
        |    parallelism-factor = 64
        |    parallelism-max = $numThreads
        |  }
        |}
        |akka {
        |  daemonic = on
        |  log-dead-letters-during-shutdown = off
        |}
        |""".stripMargin))

  // This is just documenting how you would do this (without a BalancingDispatcherConfigurator)
//  def balancingDispatcherConfWithoutConfigurator(dispatcherName: String,
//                                                 mailboxType: String = "com.gravity.utilities.DedupedUnboundedStableMailbox",
//                                                 throughput: Int = 1,
//                                                 numThreads: Int = 40) = ConfigFactory.load(ConfigFactory.parseString(
//    s"""$dispatcherName
//        |{
//        |  type = BalancingDispatcher
//        |  mailbox-type = "$mailboxType"
//        |  executor = "fork-join-executor"
//        |  throughput = $throughput
//        |  fork-join-executor {
//        |    parallelism-min = $numThreads
//        |    parallelism-factor = 64
//        |    parallelism-max = $numThreads
//        |  }
//        |}
//        |akka {
//        |  daemonic = on
//        |  log-dead-letters-during-shutdown = off
//        |}
//        |""".stripMargin))
}

