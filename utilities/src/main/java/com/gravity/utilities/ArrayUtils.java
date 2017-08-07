package com.gravity.utilities; /**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: Dec 6, 2010
 * Time: 12:12:24 PM
 */

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Adds the stuff I would like to see within the Apache Commons version this class extends
 */
public class ArrayUtils extends org.apache.commons.lang.ArrayUtils {
  private ArrayUtils() {}

  /**
   * Converts an array of type T into a List of type T.
   *
   * @param items      The source array to convert
   * @param fromIndex  low endpoint (inclusive) of the desired result
   * @param untilIndex high endpoint (exclusive) of the desired result
   * @param <T>        the element type
   * @return A List of type T containing only the elements of the specified items within the fromIndex and untilIndex
   * @throws IndexOutOfBoundsException endpoint index value out of range {@code (fromIndex < 0 || toIndex > size)}
   * @throws IllegalArgumentException  if the endpoint indices are out of order {@code (fromIndex > toIndex)}
   */
  public static <T> List<T> arrayToList(T[] items, int fromIndex, int untilIndex) {
    if (items == null) throw new IllegalArgumentException("items must not be null!");
    if (fromIndex < 0)
      throw new IndexOutOfBoundsException("fromIndex = " + fromIndex + " must not be less than zero");
    if (untilIndex > items.length)
      throw new IndexOutOfBoundsException("untilIndex = " + untilIndex + " is greater than items.length = " + items.length);
    if (fromIndex > untilIndex)
      throw new IllegalArgumentException("fromIndex(" + fromIndex +
          ") must not be > untilIndex(" + untilIndex + ")");

    List<T> results = new ArrayList<T>(untilIndex - fromIndex);
    
    results.addAll(Arrays.asList(items).subList(fromIndex, untilIndex));
    return results;
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] subArray(T[] items, int fromIndex, int untilIndex) {
    if (items == null) throw new IllegalArgumentException("items must not be null!");
    if (fromIndex < 0)
      throw new IndexOutOfBoundsException("fromIndex = " + fromIndex + " must not be less than zero");
    if (untilIndex > items.length)
      throw new IndexOutOfBoundsException("untilIndex = " + untilIndex + " is greater than items.length = " + items.length);
    if (fromIndex > untilIndex)
      throw new IllegalArgumentException("fromIndex(" + fromIndex +
          ") must not be > untilIndex(" + untilIndex + ")");

    return (T[]) ArrayUtils.subarray(items, fromIndex, untilIndex);
  }

  @SuppressWarnings("unchecked")
  public static <T> T[][] chunk(List<T> list, int sizeOfEachChunk) {
    T[] array = (T[]) Array.newInstance(list.get(0).getClass(), list.size());
    for (int i = 0; i < list.size(); i++) {
      array[i] = list.get(i);
    }
    return chunk(array, sizeOfEachChunk);
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @param <T> the type inferred from the specified array
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  @SuppressWarnings("unchecked")
  public static <T> T[][] chunk(T[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    T[][] result = (T[][]) Array.newInstance(array.getClass(), numChunks);
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = (T[]) subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static int[][] chunk(int[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    int[][] result = new int[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static long[][] chunk(long[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    long[][] result = new long[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static byte[][] chunk(byte[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    byte[][] result = new byte[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static char[][] chunk(char[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    char[][] result = new char[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static short[][] chunk(short[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    short[][] result = new short[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static double[][] chunk(double[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    double[][] result = new double[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static float[][] chunk(float[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    float[][] result = new float[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }

  /**
   *
   * @param array The full size array to chunk
   * @param sizeOfEachChunk the maximum number of elements each chunk is to contain
   * @return Returns the original array split into chunks of the specified sizeOfEachChunk size.
   *      If the length of the original array is not evenly divisible by the
   *      specified sizeOfEachChunk, then the last chunk will contain the remainder.
   */
  public static boolean[][] chunk(boolean[] array, int sizeOfEachChunk) {
    int numChunks = (array.length / sizeOfEachChunk) + (array.length % sizeOfEachChunk != 0 ? 1 : 0);
    boolean[][] result = new boolean[numChunks][];
    for (int i = 0; i < numChunks; i++) {
      int start = i*sizeOfEachChunk;
      int end = start + sizeOfEachChunk;
      result[i] = subarray(array, start, end);
    }

    return result;
  }
}


