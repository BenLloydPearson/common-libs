package com.gravity.utilities;

import java.io.File;
import java.io.IOException;

public class Directory {
	  static public boolean delete(String pathToDir) {
		  File path = new File(pathToDir);
		    if( path.exists() ) {
		      File[] files = path.listFiles();
		      for(int i=0; i<files.length; i++) {
		         if(files[i].isDirectory()) {
		           delete(files[i].getAbsolutePath());
		         }
		         else {
		           files[i].delete();
		         }
		      }
		    }
		    return( path.delete() );
		  }
	  
	  public static boolean make(String pathToDir) {
		  return new File(pathToDir).mkdirs();
	  }
	  
	  public static boolean exists(String pathToDir) {
		  return new File(pathToDir).exists();
	  }
}
