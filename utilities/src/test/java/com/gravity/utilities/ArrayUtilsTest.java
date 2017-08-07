package com.gravity.utilities; /**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: Dec 6, 2010
 * Time: 5:40:54 PM
 */

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ArrayUtilsTest {
  private static final Logger logger = LoggerFactory.getLogger(ArrayUtilsTest.class);

  @Test
  public void testArrayToList() {
    String[] array = new String[] {"0", "1", "2", "3", "4", "5"};
    List<String> results = ArrayUtils.arrayToList(array, 1, 5);
    Assert.assertNotNull("Results were NULL!", results);
    Assert.assertEquals("Incorrect number of items returned!", 4, results.size());

    int i = 1;
    for (String item : results) {
      Assert.assertEquals("This item was not the expected value when compared to the original array position!", array[i++], item);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testArrayToListOutOfRange() {
    String[] array = new String[] {"0", "1", "2", "3", "4", "5"};
    ArrayUtils.arrayToList(array, 6, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testArrayToListIllegalIndex() {
    String[] array = new String[] {"0", "1", "2", "3", "4", "5"};
    ArrayUtils.arrayToList(array, 3, 1);
  }

  @Test
  public void testChunkInt() {
    int[] array = new int[11];
    for (int i = 0; i < array.length; i++) {
      array[i] = i+1;
    }

    int[][] result = ArrayUtils.chunk(array, 5);
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.length);
    Assert.assertEquals(5, result[0].length);
    Assert.assertEquals(5, result[1].length);
    Assert.assertEquals(1, result[2].length);

    int i = 1;
    for (int[] chunk : result) {
//      logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (int item : chunk) {
//        logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  @Test
  public void testChunkLong() {
    long[] array = new long[11];
    for (int i = 0; i < array.length; i++) {
      array[i] = i+1;
    }

    long[][] result = ArrayUtils.chunk(array, 5);
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.length);
    Assert.assertEquals(5, result[0].length);
    Assert.assertEquals(5, result[1].length);
    Assert.assertEquals(1, result[2].length);

    int i = 1;
    for (long[] chunk : result) {
//      logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (long item : chunk) {
  //      logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  @Test
  public void testChunkByte() {
    String testString = "This is a test string.";

    byte[][] result = ArrayUtils.chunk(testString.getBytes(), 5);
    
    int i = 1;
    for (byte[] chunk : result) {
//      logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (byte item : chunk) {
 //       logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  @Test
  public void testChunkChar() {
    String testString = "This is a test string.";

    char[][] result = ArrayUtils.chunk(testString.toCharArray(), 5);

    int i = 1;
    for (char[] chunk : result) {
//      logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (char item : chunk) {
  //      logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  @Test
  public void testChunk() {
    ArrayItem[] array = getTestArray(11);

    ArrayItem[][] result = ArrayUtils.chunk(array, 5);
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.length);
    Assert.assertEquals(5, result[0].length);
    Assert.assertEquals(5, result[1].length);
    Assert.assertEquals(1, result[2].length);

    int i = 1;
    for (ArrayItem[] chunk : result) {
//      logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (ArrayItem item : chunk) {
  //      logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  @Test
  public void testChunkWithList() {
    List<ArrayItem> list = getTestList(11);
    ArrayItem[][] result = ArrayUtils.chunk(list, 5);
    Assert.assertNotNull(result);
    Assert.assertEquals(3, result.length);
    Assert.assertEquals(5, result[0].length);
    Assert.assertEquals(5, result[1].length);
    Assert.assertEquals(1, result[2].length);

    int i = 1;
    for (ArrayItem[] chunk : result) {
     // logger.info("Chunk #{} has {} items", i++, chunk.length);
      for (ArrayItem item : chunk) {
       // logger.info("\t{}", item);
        Assert.assertNotNull(item);
      }
    }
  }

  private ArrayItem[] getTestArray(int size) {
    ArrayItem[] array = new ArrayItem[size];
    for (int i = 1; i <= array.length; i++) {
      array[i-1] = new ArrayItem(i, "Item " + i);
    }
    return array;
  }

  private List<ArrayItem> getTestList(int size) {
    List<ArrayItem> result = new ArrayList<ArrayItem>(size);
    for (int i = 1; i <= size; i++) {
      result.add(new ArrayItem(i, "Item " + i));
    }
    return result;
  }

  class ArrayItem {
    public int id;
    public String name;

    ArrayItem(int id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}


