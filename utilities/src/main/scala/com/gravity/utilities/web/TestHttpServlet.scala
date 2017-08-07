package com.gravity.utilities.web

import javax.servlet.http.HttpServletRequest

import org.scalatra.Params
import org.scalatra.util.{MapWithIndifferentAccess, MultiMapHeadView}

trait TestHttpServlet extends BaseGravityServlet

/** For tests requiring dummy params. */
case class TestHttpServletWithDummyParams(dummyParams: Map[String, String] = Map[String, String]())  extends TestHttpServlet {
  override def params(implicit request: HttpServletRequest): MultiMapHeadView[String, String] with MapWithIndifferentAccess[String] with Object {def multiMap: Map[String, Seq[String]]} = new MultiMapHeadView[String, String] with MapWithIndifferentAccess[String] {
    protected def multiMap = dummyParams.mapValues(Seq(_))
  }
}