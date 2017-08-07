package com.gravity.utilities;

import org.slf4j.*;

import java.io.*;

public class Streams {

  public interface PerLine {
    public void line(String line);
  }

  private static final Logger logger = LoggerFactory.getLogger(Streams.class);

  public static void perLine(InputStream is, PerLine perlineImpl) {
    try {
      String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        while ((line = reader.readLine()) != null) {
            perlineImpl.line(line);
        }
        is.close();
    } catch(IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  public static String getResource(String path,Class cls) {
    InputStream is = cls.getResourceAsStream(path);
    String result = streamToString(is);
    
    return result;
  }

  public static InputStream fromRootOrResource(String path,Class cls) {
    if(new File(path).exists()) {
      try{
        return new FileInputStream(path);
      }catch(FileNotFoundException exc) {
        throw new RuntimeException(exc);
      }
    }else {
      return cls.getResourceAsStream(path);
    }
  }

	public static String streamToString(InputStream is)  {
      StringBuilder sb = new StringBuilder();
      String line;

      try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
          while ((line = reader.readLine()) != null) {
              sb.append(line).append("\n");
          }
          is.close();
      } catch(IOException exc) {
        throw new RuntimeException(exc);
      }

      return sb.toString();
	}

  public static OutputStream stringBuilderToOutputStream(StringBuilder sb) {
    byte[] buf = sb.toString().getBytes();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(buf.length);
    try {
      baos.write(buf);
      return baos;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
