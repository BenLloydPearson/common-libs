package com.gravity.utilities.web

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.gravity.test.utilitiesTesting
import com.gravity.utilities.api.BootstrapServletTestBase
import com.gravity.utilities.web.http.{optOutCookieName, optOutCookieValue}
import org.mockito.Mockito._
import org.scalatra.{ScalatraServlet, Cookie}

class BeaconingTest extends BootstrapServletTestBase with utilitiesTesting {


  test("correct first-party cookie") {
    implicit val req = mock[HttpServletRequest]
    implicit val resp = mock[HttpServletResponse]

    val legitUserGuid1 = "deadbeef01234567deadbeef01234567"
    val legitUserGuid2 = "00000000000000000000000000000000"
    val illegitUserGuid = legitUserGuid1 + "000"

    val oneCookie = Array(
      Cookie("vaguid", legitUserGuid1)
    ) map Beaconing.toServletCookie

    val optedOutTho = oneCookie :+ Beaconing.toServletCookie(Cookie(optOutCookieName, optOutCookieValue))

    val multipleCookies1First = Array(
      Cookie("vaguid", illegitUserGuid),
      Cookie("vaguid", legitUserGuid1),
      Cookie("vaguid", legitUserGuid2)
    ) map Beaconing.toServletCookie

    val multipleCookies2First = Array(
      Cookie("messingWithYou", "kanyeAtTheVMAs"),
      Cookie("vaguid", legitUserGuid2),
      Cookie("vaguid", legitUserGuid1)
    ) map Beaconing.toServletCookie

    when (req.getCookies) thenReturn oneCookie
    Beaconing.userGuidFromCookie should equal (Some(legitUserGuid1))

    when (req.getCookies) thenReturn optedOutTho
    Beaconing.userGuidFromCookie should be ('empty)

    when (req.getCookies) thenReturn multipleCookies1First
    Beaconing.userGuidFromCookie should equal (Some(legitUserGuid1))

    when (req.getCookies) thenReturn multipleCookies2First
    Beaconing.userGuidFromCookie should equal (Some(legitUserGuid2))
  }

}
