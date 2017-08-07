package com.gravity.interests.jobs.intelligence.ui.recommendation;

import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BostonWidgetFrameTest {
  @Test
  public void gravityWidgetIFrame() {
    Cookie[] cookies = new Cookie[3];
    cookies[0] = mock(Cookie.class);
    cookies[1] = mock(Cookie.class);
    cookies[2] = mock(Cookie.class);
    
    when(cookies[1].getName()).thenReturn("grvinsights");
    when(cookies[1].getValue()).thenReturn("joe");
    
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getCookies()).thenReturn(cookies);
    when(req.getRequestURL()).thenReturn(new StringBuffer("http://boston.com/foo/bar"));

    String expectedIframe = "<iframe src=\"http://rma-api.gravity.com/v1/api/intelligence/w/fe6d5479cc3242c2b7ef253225b4bfcc/joe?sourceUrl=http://boston.com/foo/bar\" frameborder=\"0\" scrolling=\"no\" style=\"overflow: hidden; width: 100%; height: 441px;\"></iframe>";
    String iframe = BostonWidgetFrame.gravityWidgetIFrame(req);
    Assert.assertEquals(expectedIframe, iframe);
  }
}
