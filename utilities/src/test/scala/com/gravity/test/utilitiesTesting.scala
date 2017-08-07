package com.gravity.test

import com.gravity.utilities.{BeforeAndAfterListeners, BaseScalaTest}
import org.scalatest.Suite

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Shared testing functionality to mix into utilities
 */
trait utilitiesTesting extends Suite with BeforeAndAfterListeners with TestFunctionPackage {

  /**
   * Here's an example of an object that holds test data.  Almost always wrap test data in these objects.  Will help refactoring later.
   *
   * For example if you have an ArticleRow, wrap it in an ArticleContext.
   * @param name
   */
  case class ExampleTestDataContext(name:String)

  case class ExampleDependentDataContext(nestedContext:ExampleTestDataContext, extraData:String)

  /**
   * This is a template of a function that
   * @param param
   * @param work
   * @tparam T
   * @return
   */
  def withExampleDataContext[T](param:String)(work: (ExampleTestDataContext) => T) : T = {
    val context = ExampleTestDataContext(param)
    work(context)
  }

  def withNestedDataContext[T](param:String, context: ExampleTestDataContext)(work: (ExampleDependentDataContext) => T) : T = {
    val newContext = ExampleDependentDataContext(context, param)

    work(newContext)
  }
}

