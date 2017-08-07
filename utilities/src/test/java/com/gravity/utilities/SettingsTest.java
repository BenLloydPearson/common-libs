//package com.gravity.utilities;
///**
// * Created by IntelliJ IDEA.
// * User: robbie
// * Date: Jun 25, 2010
// * Time: 10:24:35 AM
// */
//
//import org.apache.commons.lang3.time.StopWatch;
//import org.junit.Assert;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import com.gravity.utilities.Settings._
//import java.util.Properties;
//
//public class SettingsTest {
//  private static Logger logger = LoggerFactory.getLogger(SettingsTest.class);
//
//  @Test
//  public void testRoles() {
//
//    Assert.assertNotNull(Settings.APPLICATION_ROLE);
//    Assert.assertNotNull(Settings.CANONICAL_HOST_NAME);
//  }
//
//
//  @Test
//  public void testLoadSettings() {
//    Assert.assertNotNull(Settings.APPLICATION_ENVIRONMENT);
//    println("Application Environment: " + Settings.APPLICATION_ENVIRONMENT);
//    Assert.assertTrue(Settings.getPropertyNames().size() > 100);
//  }
//
//  @Test
//  public void testRoleOverride() {
//    String role = "ROLE1";
//    Settings.APPLICATION_ROLE = role;
//
//    Properties props = new Properties();
//    props.put("foo.bar", "just blaze");
//    props.put("foo.bar.ROLE1", "kanye west");
//    props.put("foo.baz." + System.getProperty("user.name"), "nas");
//    props.put("foo.baz", "jay-z");
//    props.put("foo.baz.ROLE1", "biggie");
//    Settings.GravityRoleProperties gravityProps = new Settings.GravityRoleProperties(props);
//
//    gravityProps.validateApplicationRolePropertyOverrides();
//
//    Assert.assertEquals("kanye west", gravityProps.getProperty("foo.bar"));
//    Assert.assertEquals("nas", gravityProps.getProperty("foo.baz"));
//
//    for (String prop : gravityProps.getPropertyNames()) {
//      System.setProperty(prop, gravityProps.getProperty(prop));
//    }
//
//    Assert.assertEquals("kanye west", System.getProperty("foo.bar"));
//    Assert.assertEquals("nas", System.getProperty("foo.baz"));
//  }
//
////  @Test(expected = IllegalArgumentException.class)
////  public void testInvalidRoleOverride() {
////    String role = "ROLE1";
////    Settings.APPLICATION_ROLE = role;
////
////    Properties props = new Properties();
////    props.put("foo.bar.ROLE1", "true");
////    props.put("foo.bar.ROLE2", "false");
////    Settings.GravityRoleProperties pl = new Settings.GravityRoleProperties(props);
////
////    pl.validateApplicationRolePropertyOverrides();
////  }
//
//  @Test
//  @Ignore
//  public void benchmarkGetProperty() {
//    // warm-up
//    int warmupIterations = 1000;
//
//    for (int i = 0; i < warmupIterations; i++) {
//      Settings.isInMaintenanceMode();
//    }
//
//    int iterations = 10000000;
//
//    println("Warm-up complete. We'll now run our benchmark for %0,1d iterations...", iterations);
//
//    StopWatch sw = new StopWatch();
//    sw.start();
//
//    for (int i = 0; i < iterations; i++) {
//      Settings.isInMaintenanceMode();
//    }
//
//    sw.stop();
//
//    println("Benchmark completed in: %s", sw.toString());
//  }
//
//  private void println(Object value) {
//    System.out.println(value);
//  }
//
//  private void println(String value) {
//    System.out.println(value);
//  }
//
//  private void println(String format, Object... args) {
//    println(String.format(format, args));
//  }
//}
//
