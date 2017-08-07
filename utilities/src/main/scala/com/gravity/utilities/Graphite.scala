package com.gravity.utilities

import java.net.{InetSocketAddress, SocketAddress, Socket}
import java.io.{IOException, DataOutputStream}

/**
 * User: mtrelinski
 */

class Graphite(val server: String, val port: Int, val ts: Long, val timeoutMs: Int) {

  private def putRaw(data: String) {
    val socket = new Socket()
    val socketAddress: SocketAddress = new InetSocketAddress(server, port)
    socket.connect(socketAddress, timeoutMs)
    if (socket.isConnected) {
      val out = new DataOutputStream(socket.getOutputStream())
      out.write((data + "\r\n").getBytes)
      out.flush()
      out.close()
      socket.close()
    } else {
      throw new IOException("Could not connect to graphite!")
    }
  }

  def putMetric(metric: String, value: Any): Unit = putRaw(metric + " " + value.toString + " " + ts.toString)

}
