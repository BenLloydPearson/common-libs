package com.gravity.utilities.web.json

import com.gravity.utilities.grvenum.GrvEnum
import play.api.libs.json._

import scala.reflect.runtime.universe._
import scala.util.{Try => UtilTry}

object JsonSchemaBuilder {

  type JsonSchemaProperty = (String, JsValue)
  type RequiredNamesWithJsValues = (Seq[String], JsObject)
  case class JsSchemaWithRequiredFields(schema: JsValue, requiredFields: Seq[String])

  final case class JsonableSchema(name: String, typeName: String) {
    def toJson: JsValue = {
      Json.obj(
        name -> Json.obj("type" -> JsString(typeName))
      )
    }
  }

  // Note: is this is needed
//  private def dealiasParams(t: Type): Type = {
//    appliedType(t.dealias.typeConstructor, t.typeArgs.map { arg ⇒
//      dealiasParams(arg.dealias)
//    })
//  }

  private def higherOrderType(higherOrder: String, typeName: String): Option[String] = {
    s"$higherOrder\\[(\\S+)\\]".r.findFirstMatchIn(typeName).map(_.group(1))
  }

  private def getJsonSchemaType(`type`: String, isRequired: Boolean = true): JsValue = {
    `type` match {
      case "Int" | "int" => JsObject(List("type" -> JsString("integer"), "format" -> JsString("int32"), "required" -> JsBoolean(isRequired)))
      case "Long" | "long" => JsObject(List("type" -> JsString("integer"), "format" -> JsString("int64"), "required" -> JsBoolean(isRequired)))
      case "String" | "string" => JsObject(List("type" -> JsString("string"), "required" -> JsBoolean(isRequired)))
      case "Double" | "BigDecimal" => JsObject(List("type" -> JsString("number"), "format" -> JsString("double"), "required" -> JsBoolean(isRequired)))
      case "Float" => JsObject(List("type" -> JsString("number"), "format" -> JsString("float"), "required" -> JsBoolean(isRequired)))
      case "DateTime" => JsObject(List("type" -> JsString("integer"), "format" -> JsString("epoch"), "required" -> JsBoolean(isRequired)))
      case "Any" => JsObject(List("type" -> JsString("any"), "required" -> JsBoolean(isRequired)))
      case unknown => JsObject(List("type" -> JsString(unknown.toLowerCase)))
    }
  }

  private def getJsonSchemaObject(clazz: Class[_]): JsValue = {
    val declaredFields = clazz.getDeclaredFields
    val properties = declaredFields.map { field =>
      val name = field.getName
      val typeName = field.getType.getName
      JsonableSchema(name, typeName)
    }

    val result = properties map format

    JsObject(result)
  }

  def grvEnumValues(typeName: String): Option[Set[String]] = {
    val updatedTypeName = typeName.replaceAll("\\.Type", "\\$")

    UtilTry {
      val cl = Class.forName(updatedTypeName)
      val singleton = cl.getField("MODULE$").get(cl)
      val enumValues = singleton.asInstanceOf[GrvEnum[_]].valuesMap.keySet
      enumValues
    }.toOption.map(_.toSet)
  }

  // This will not work with classes that are nested in other classes, it doesn't pass the fully qualified name.
  def oneOfOurClasses(typeName: String): Option[Class[_]] = {
    UtilTry {
      Class.forName(typeName)
    }.toOption
  }

  private def format(js: JsonableSchema): JsonSchemaProperty = {
    val typeName = js.typeName.replaceAll("(scala.)|(java.lang.)|(math.)|(org.joda.time.)", "")
    val optionalType = higherOrderType("Option", typeName)
    val enumValues = grvEnumValues(typeName)
    val ourClass = oneOfOurClasses(typeName)

    if (optionalType.isDefined) {
      val optTypeValue = optionalType.get
      val optClass = oneOfOurClasses(optTypeValue)
      val optClassJsValue = optClass.map(xyz => getJsonSchemaObject(xyz))
      val optEnumValues = grvEnumValues(optTypeValue)

      if (optClassJsValue.isDefined) {
        js.name -> optClassJsValue.get
      } else if (optEnumValues.isDefined) {
        val jsStringEnumValues = optEnumValues.get.map(s => JsString(s)).toList
        js.name -> JsObject(List("type" -> JsString("string"), "required" -> JsBoolean(false), "enum" -> JsArray(jsStringEnumValues)))
      } else {
        js.name -> getJsonSchemaType(optionalType.get, isRequired = false)
      }
    } else if (enumValues.isDefined) {
      val jsStringEnumValues = enumValues.get.map(s => JsString(s)).toList
      js.name -> JsObject(List("type" -> JsString("string"), "required" -> JsBoolean(true), "enum" -> JsArray(jsStringEnumValues)))
    } else if (ourClass.isDefined) {
      js.name -> getJsonSchemaObject(ourClass.get)
    } else {
      js.name -> getJsonSchemaType(typeName, isRequired = true)
    }
  }

  private def buildWithRequiredNames(tpe: Type): RequiredNamesWithJsValues = {
    val fields = tpe.decls.collectFirst {
      case m: MethodSymbol if m.isPrimaryConstructor ⇒ m
    }.toList.flatMap(_.paramLists).headOption.getOrElse(Nil)

    val properties = fields.map { field ⇒
      val name = field.name.decodedName.toString
      val typeName = field.typeSignature.toString
      JsonableSchema(name, typeName)
    }

    val jsonSchemaProperties = properties map format

    val requiredPropertyNames = jsonSchemaProperties.filter { case (n, js) =>
      val isRequiredOpt = (js \ "required").asOpt[Boolean]
      isRequiredOpt.getOrElse(false)
    }.map { case (k, v) => k }

    requiredPropertyNames -> JsObject(jsonSchemaProperties)
  }

  def build(tpe: Type): JsObject = {
    buildWithRequiredNames(tpe)._2
  }

  def requiredFields(tpe: Type): Seq[String] = {
    buildWithRequiredNames(tpe)._1
  }

}
