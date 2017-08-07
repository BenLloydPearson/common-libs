package com.gravity.interests.jobs.intelligence.querybuilder

import com.gravity.utilities.grvfunc._
import play.api.libs.json.Json._
import play.api.libs.json._

import scala.collection._
import scala.collection.immutable.ListSet
import scala.collection.mutable.ListBuffer

/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 *
 *
 *  CriteriaBuilder allows us to define a model that can be used with jQuery QueryBuilder to interactively build
 *  criteria rules that can be saved and applied at runtime.  see: http://querybuilder.js.org/
 *
 *  !!! For example usage, see SegmentResolver.CriteriaBuilder and tests in CriteriaBuilderTest !!!
 *
 *  A rough outline on how the code is structured:
 *
 *   CriteriaBuilder (should be subclassed to provide fields that can be used to build criteria and build the criteria itself)
 *     -> Field[]
 *       -> ValueType (represents the field's value type (name, json format, etc)
 *       -> InputType (represents the field's input type used to edit criteria for the field [select, text, checkbox, etc])
 *       -> HasOperators (implicitly) binds the operators that are allowed for given ValueType and InputType
 *         -> CanBeOperator[] (operator implementations for the given ValueType)
 *
 *   Criteria (recursive structure of Group/Field criteria instances)
 *     -> Group
 *       -> Condition (AND/OR)
 *       -> Criteria[] (sub-criteria joined by 'Condition')
 *     -> Field
 *       -> CriteriaBuilder.Field (field used for operand)
 *       -> Operator (operator and arguments)
 */

trait CriteriaBuilder[T] {

  import Criteria._

  def and(rules: (this.type => Criteria[T])*): Group[T] = Group[T](And, rules.map(f => f(this)).toList)
  def or(rules: (this.type => Criteria[T])*): Group[T] = Group[T](Or, rules.map(f => f(this)).toList)
  def field[FT, O <: Operator[FT]](field: this.type => Field[FT], op: Operator[FT]): Criteria.Field[T, FT] = Criteria.Field(field(this), op)

  implicit def criteriaJsonFormat: Format[Criteria[T]] = Criteria.jsonFormat[T](this)

  case class Field[FT] protected[querybuilder] (accessor: T => FT, name: String, label: String, valueType: ValueType[FT], inputType: InputType[FT], operators: Seq[CanBeOperator[_ <: Operator[FT], FT]], hidden: Boolean = false) {
    def operatorNames: Seq[String] = operators.map(_.name)
  }

  private val fields: ListBuffer[Field[_ <: Any]] = ListBuffer.empty

  final def addField[IT[x] <: InputType[x], FT](accessor: T => FT, name: String, label: String, inputType: IT[FT], hidden: Boolean = false)(implicit valueType: ValueType[FT], ho: HasOperators[IT, FT]): Field[FT] = {
    val field = Field(accessor, name, label, valueType, inputType, ho.operators, hidden)
    fields += field
    field
  }

  final def getField(name: String): Option[Field[ _ <: Any]] = fields.find(_.name == name)
}


object CriteriaBuilder {

  implicit def jsonWrites[T]: Writes[CriteriaBuilder[T]] = Writes {
    case m => Json.obj(
      "filters" -> m.fields.filterNot(_.hidden).map(f => {
        f.inputType.configure(Json.obj(
          "id" -> f.name,
          "field" -> f.name,
          "label" -> f.label,
          "type" -> f.valueType.name,
          "input" -> f.inputType.name,
          "operators" -> f.operatorNames
        ))
      })
    )
  }

}
