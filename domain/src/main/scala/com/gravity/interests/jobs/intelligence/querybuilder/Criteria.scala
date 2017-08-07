package com.gravity.interests.jobs.intelligence.querybuilder

import com.gravity.utilities.Predicates._
import com.gravity.utilities.grvjson._
import play.api.libs.json.Json._
import play.api.libs.json._

import scala.collection._
import scala.collection.immutable.ListSet

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

sealed trait Condition {val name: String }
case object Or extends Condition { val name = "OR" }
case object And extends Condition {val name = "AND" }

sealed trait Criteria[T] {
  def predicate: T => Boolean
}

object Criteria extends GroupCriteria with FieldCriteria {

  implicit def equals[T]: scalaz.Equal[Criteria[T]] = new scalaz.Equal[Criteria[T]] {
    override def equal(a1: Criteria[T], a2: Criteria[T]): Boolean = a1 == a2
  }

  def jsonFormat[T](model: CriteriaBuilder[T]): Format[Criteria[T]] = Format(
    Reads {
      case js: JsValue if (js \ "condition").asOpt[String].isDefined => Group.jsonFormat(model).reads(js)
      case js: JsValue if (js \ "id").asOpt[String].isDefined => model.getField((js \ "id").as[String]) match {
        case Some(field) => Field.jsonFormat(field).reads(js)
        case None => JsError(s"Field '" + (js \ "id") + "' not defined for model!")
      }
    },
    Writes {
      case v: Group[T @unchecked] => Group.jsonFormat(model).writes(v)
      case f: Field[T @unchecked, Any @unchecked] => Field.jsonFormat(f.field).writes(f)
      case v => throw new MatchError(s"Un-expected criteria: $v")
    }
  )
}

trait GroupCriteria {

  case class Group[T](condition: Condition, criterias: List[Criteria[T]] = List.empty[Criteria[T]]) extends Criteria[T] {
    def predicate: T => Boolean = condition match {
      case And => criterias.foldLeft((_ => true): T => Boolean)((acc, cur) => acc and cur.predicate)
      case Or => criterias.foldLeft((_ => false): T => Boolean)((acc, cur) => acc or cur.predicate)
    }
    override def toString = {

      if (criterias.size == 1) {
        // for a single criteria, simplify the string
        criterias.head.toString
      } else {
        "(" + criterias.mkString(s" ${condition.name.toLowerCase} ") + ")"
      }
    }
  }

  object Group {

    def jsonFormat[T](model: CriteriaBuilder[T]): Format[Group[T]] = {
      implicit val ruleFormat: Format[Criteria[T]] = Criteria.jsonFormat(model)

      Format(
        Reads {
          case json: JsValue => for {
            cond <- (json \ "condition").validate[String]
            condition <- cond.toUpperCase() match {
              case "AND" => And.jsSuccess
              case "OR" => Or.jsSuccess
              case p => JsError(s"Unknown condition operator '$p'")
            }
            rules <- (json \ "rules").validate[List[Criteria[T]]]
          } yield Group[T](condition, rules)
        },
        Writes {
          case v => Json.obj(
            "condition" -> v.condition.name,
            "rules" -> Json.toJson(v.criterias)
          )
        }
      )
    }
  }
}


trait FieldCriteria {

  case class Field[T, FT](val field: CriteriaBuilder[T]#Field[FT], val operator: Operator[FT]) extends Criteria[T] {
    override def predicate: T => Boolean = (t: T) => operator.predicate(field.accessor(t))
    override def toString: String = field.name + "." + operator.toString(field.valueType)
  }

  object Field {

    def jsonFormat[T, FT](field: CriteriaBuilder[T]#Field[FT]): Format[Field[T, FT]] = {

      implicit val vt: ValueType[FT] = field.valueType

      Format(
        Reads {
          case json: JsValue => for {
            opName <- (json \ "operator").validate[String]
            oper <- field.operators.find(_.name == opName).map(cbo => (json \ "value").validate(cbo.jsonFormat)).getOrElse(JsError(s"Operator '$opName' not supported for field '${field.name}' with type '${field.valueType.name}'"))
          } yield Field[T, FT](field, oper)
        },
        Writes {
          case r => obj(
            "id" -> r.field.name,
            "field" -> r.field.name,
            "type" -> r.field.valueType.name,
            "operator" -> r.operator.name,
            "value" -> r.operator.jsonFormat.writes(r.operator)
          )
        }
      )
    }
  }

}
