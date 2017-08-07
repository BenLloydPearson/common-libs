package com.gravity.interests.jobs.intelligence.ui.recommendation;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

public class BostonWidgetFrame {
  /**
   * @return the HTML to render the Gravity Personalization iframe
   */
  public static String gravityWidgetIFrame(HttpServletRequest request) {
    String gravityUserId = "";
    for (Cookie c : request.getCookies()) {
      if ("grvinsights".equals(c.getName())) {
        gravityUserId = c.getValue();
        break;
      }
    }

    String currentPageUrl = request.getRequestURL().toString();
    return "<iframe src=\"http://rma-api.gravity.com/v1/api/intelligence/w/fe6d5479cc3242c2b7ef253225b4bfcc/" + gravityUserId + "?sourceUrl=" + currentPageUrl + "\" frameborder=\"0\" scrolling=\"no\" style=\"overflow: hidden; width: 100%; height: 441px;\"></iframe>";
  }
}
