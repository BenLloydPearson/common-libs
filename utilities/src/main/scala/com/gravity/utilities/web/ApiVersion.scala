package com.gravity.utilities.web

/**
  * Created by tdecamp on 5/24/16.
  * {{insert neat ascii diagram here}}
  */
sealed trait ApiVersion {
  val major: Int
  val minor: Int
}

object ApiVersion {
  val apiVersions = List(ApiVersion0_0, ApiVersion1_0)
  val apiVersionsDesc = List(ApiVersion1_0, ApiVersion0_0)

  def fromMajorVersion(int: Int): Option[ApiVersion] = {
    int match {
      case 0 => Some(ApiVersion0_0)
      case 1 => Some(ApiVersion1_0)
      case _ => None
    }
  }
}

case object ApiVersion0_0 extends ApiVersion {
  override val major: Int = 0
  override val minor: Int = 0
}

case object ApiVersion1_0 extends ApiVersion {
  override val major: Int = 1
  override val minor: Int = 0
}
