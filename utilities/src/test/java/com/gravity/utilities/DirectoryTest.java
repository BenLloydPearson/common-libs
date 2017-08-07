//package com.gravity.utilities;
//
//import org.junit.Assert;
//import junit.framework.TestCase;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//
//public class DirectoryTest extends TestCase {
//
//
//
//	String baseDir = "/tmp/test/";
//
//	public void setUp() throws IOException {
//
//    Settings.UNIT_TESTING = true;
//			File dir = new File(baseDir);
//
//		if(!dir.exists()){
//			dir.mkdirs();
//		}
//
//		File testFile = new File(baseDir + "test.txt");
//
//		if(!testFile.exists()) {
//			FileWriter writer = new FileWriter(testFile);
//			writer.write("mary had a little lamb");
//			writer.close();
//		}
//	}
//
//	public void testSettings() {
//    System.out.println(Settings.APPLICATION_ENVIRONMENT);
//  }
//
//	public void testDeleteDirectory() {
//
//		Assert.assertTrue(new File(baseDir).exists());
//		Assert.assertTrue(new File(baseDir + "test.txt").exists());
//
//		Directory.delete(baseDir);
//
//		Assert.assertFalse(new File(baseDir).exists());
//	}
//
//}
