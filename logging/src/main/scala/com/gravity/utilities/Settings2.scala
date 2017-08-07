package com.gravity.utilities

import java.io.{File, IOException}
import java.util.Properties
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

/**
 * Add new functionality and port old functionality for Settings.java here.
 */
object Settings2 {
  val logger = LoggerFactory.getLogger("com.gravity.utilities.Settings2")
  val INTEREST_SERVICE_USER_ID: Long = -2525l
  lazy val role: String = Settings.APPLICATION_ROLE
  val ROLE_DEVELOPMENT = "DEVELOPMENT"

  //a small subset of what's in grvstrings. moving all of that to this project pulled too much with it
  implicit class SettingString(val orig: String) extends AnyVal {
    def tryToInt: Option[Int] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toInt)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToLong: Option[Long] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toLong)
      } catch {
        case _: NumberFormatException => None
      }
    }

    def tryToBoolean: Option[Boolean] = {
      if (orig == null || orig.isEmpty) return None

      orig.toLowerCase match {
        case "1" => return Some(true)
        case "0" => return Some(false)
        case "on" => return Some(true)
        case "off" => return Some(false)
        case _ =>
      }

      try {
        Some(orig.toBoolean)
      } catch {
        case _: NumberFormatException => None
        case _: IllegalArgumentException => None
      }
    }

    def tryToDouble: Option[Double] = {
      if (orig == null || orig.isEmpty) return None
      try {
        Some(orig.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    }
  }

  def getProperty(key: String): Option[String] = Option(Settings.getProperty(key))

  def getPropertyOrDefault(key: String, default: String): String = getProperty(key) match {
    case Some(value) => value
    case None => default
  }
  
  def getBoolean(name: String): Option[Boolean] = getProperty(name).flatMap(_.tryToBoolean)

  def getBooleanOrDefault(name: String, default: Boolean): Boolean = getBoolean(name).getOrElse(default)

  def getInt(name: String): Option[Int] = getProperty(name).flatMap(_.tryToInt)

  def getIntOrDefault(name: String, default: Int): Int = getInt(name).getOrElse(default)

  def getLong(name: String): Option[Long] = getProperty(name).flatMap(_.tryToLong)

  def getLongOrDefault(name: String, default: Long): Long = getLong(name).getOrElse(default)

  def getDouble(name: String): Option[Double] = getProperty(name).flatMap(_.tryToDouble)

  def getDoubleOrDefault(name: String, default: Double): Double = getDouble(name).getOrElse(default)

  def isInMaintenanceMode: Boolean = getBooleanOrDefault("recommendations.maintenance", default = false)

  def withMaintenance[A](default: => A)(work: => A) : A = isInMaintenanceMode match {
    case true => default
    case false => work
  }

  def isDevelopmentRole: Boolean = role == ROLE_DEVELOPMENT

  def isDevelopmentEnvironment: Boolean = Settings.APPLICATION_ENVIRONMENT == "dev"

  def isProductionServer: Boolean = {
    getBooleanOrDefault("operations.is.production.server", Settings.isProductionServer)
  }

  def isTest = Settings.isTest
  def isAws: Boolean = Settings.isAws

  def isHadoopServer: Boolean = Settings.isHadoopServer

  def opsOverrideProperties: Properties = {

    val props = new Properties()

    val opsOverridePropertiesFile = new File("/etc/gravity/interestservice/override.properties")
    if (opsOverridePropertiesFile.exists) {

      try {
        props.load(FileUtils.openInputStream(opsOverridePropertiesFile))
      }
      catch {
        case ioe: IOException => logger.error("Ops override.properties exists but couldn't read", ioe)
      }

    }

    props
  }

  def opsOverrideRole: String = {

    val roleToBeForced = opsOverrideProperties.getProperty("application.role", opsOverrideProperties.getProperty("application.roles", ""))

    if (roleToBeForced.nonEmpty) {
      logger.info("Role being forced by ops override.properties: " + roleToBeForced)
      roleToBeForced
    }
    else {
      ""
    }
  }

  def webRoot: String = getPropertyOrDefault("application.webroot", "")

  /** @return Whether the given role requires assets.manifest to function normally. */
  def roleRequiresAssetsManifest(role: String): Boolean = rolesRequiringAssetsManifest.contains(role)

  lazy val rolesRequiringAssetsManifest = Settings2.getPropertyOrDefault("assets.manifest.requiredByRoles", "")
    .split(',')
    .toSet
}