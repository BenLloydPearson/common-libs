package com.gravity.service

import com.gravity.service.remoteoperations.RemoteOperationsServer

abstract class ServerRole {
  private var isWarm = true
  var isTerminated = false
  var isStarted = false
  var settingsRolename = ""

  def setSettingsRoleName(name: String) {
    settingsRolename = name
  }

  def setIsStarted(isStarted : Boolean) {
    this.isStarted = isStarted
  }

  protected def setIsWarm(isWarm: Boolean): Unit = {
    this.isWarm = isWarm
  }

  def getSettingsRoleName: String = settingsRolename
  def getIsStarted: Boolean = isStarted
  def getRoleName: String = getClass.getName
  def getIsWarm: Boolean = isWarm

  def start()

  def stop() {} //override this to perform role specific shut down actions

  def getRemoteOperationsServerOpt : Option[RemoteOperationsServer]
}

