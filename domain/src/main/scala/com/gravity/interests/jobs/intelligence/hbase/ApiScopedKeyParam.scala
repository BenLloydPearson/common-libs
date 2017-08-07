package com.gravity.interests.jobs.intelligence.hbase

import com.gravity.utilities.components.FailureResult
import com.gravity.utilities.grvz._
import com.gravity.utilities.swagger.adapter.ParameterType
import com.gravity.utilities.web._

import scalaz.ValidationNel

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

case class ApiScopedKeyParam(key: String, required: Boolean, description: String, defaultValue: ScopedKey = null,
                             exampleValue: String = "ck_._siteId_-5945645506992120012+campaignId_1586630572045209781")
  extends ApiParam[ScopedKey](key, required, description, defaultValue, exampleValue,
                              typeDescription = "A string parameter. The serialized version of a Scoped Key.") {

  private lazy val scopedKeySpec: (ApiRequest) => ValidationNel[FailureResult, ScopedKey] = (request: ApiRequest) =>
    for {
      maybeScopedKey <- request.get(key).toValidationNel(FailureResult(s"Key ${key} not found in request"))
      scopedKey <- ScopedKey.validateKeyString(maybeScopedKey).toValidationNel
    } yield scopedKey


  override def apply(implicit request: ApiRequest): Option[ScopedKey] = scopedKeySpec(request).toOption
  override def validate(request: ApiRequest, paramTypeDisplayName: String): Option[(String, ValidationCategory)] =
    super.validate(request, paramTypeDisplayName) match {
      case validation@Some(_) => validation
      case None => scopedKeySpec(request).leftMap(fails => (fails.map(_.message).mkString("; "), ValidationCategory.ParseOther)).swap.toOption
    }

  override def swaggerType: ParameterType = ParameterType.string
}