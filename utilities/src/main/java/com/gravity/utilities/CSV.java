package com.gravity.utilities; /**
 * User: chris
 * Date: May 13, 2010
 * Time: 9:38:44 AM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class CSV {
  private static Logger logger = LoggerFactory.getLogger(CSV.class);

  public interface ValueReader {
    public void item(int index, String item);
  }

  public interface NoHeaderLineReader {
    public void line(int lineNumber, String[] values);
  }

  public interface LineReader {
    public void line(int lineNumber, Map<String, String> values);
  }

  public enum Options {
    TRIMQUOTES
  }

  public static void read(InputStream stream, LineReader csvreader, Options... options) {
    read(stream, ",", csvreader, options);
  }

  public static void read(InputStream stream, NoHeaderLineReader lineHandler, Options... options) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      String line;
      int lineNumber = 0;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        lineHandler.line(lineNumber, line.split(","));
      }

      reader.close();
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  /**
   * Reads a csv file with headers, calls LineReader once with a table of header to value
   *
   * @param stream
   * @param csvreader
   */
  public static void read(InputStream stream, String delimiter, LineReader csvreader, Options... options) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      Map<Integer, String> headers = new HashMap<Integer, String>();

      int lineNumber = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        HashMap<String, String> lineValues = new HashMap<String, String>();
        StringTokenizer st = new StringTokenizer(line, delimiter);
        int index = 0;
        while (st.hasMoreTokens()) {
          String token = st.nextToken();
          if (options[0] == Options.TRIMQUOTES) {
            token = token.replaceAll("\"", "");
          }
          if (lineNumber == 1) {
            headers.put(index, token);
          } else {
            lineValues.put(headers.get(index), token);
          }
          index++;
        }
        if (lineNumber > 1) {
          csvreader.line(lineNumber, lineValues);
        }
      }

    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }


  public static void read(InputStream stream, ValueReader csvreader) {
    read(stream, ",", csvreader);
  }

  /**
   * Reads a stream assuming comma delimited, calls ValueReader once per value with column offset
   *
   * @param stream
   * @param csvreader
   */
  public static void read(InputStream stream, String delimiter, ValueReader csvreader) {
    try {

      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
      String line;
      while ((line = reader.readLine()) != null) {
        String[] itms = line.split(delimiter);
        for (int i = 0; i < itms.length; i++) {
          csvreader.item(i, itms[i]);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
