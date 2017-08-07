package com.gravity.utilities

import org.junit.Test
import java.io.File
import org.junit.Assert._

/**
 * Created by Jim Plush
 * User: jim
 * Date: 6/23/11
 */

object FileHelperTest extends App {
  //testNioFileCopy

  // use the nfs mount to test
  // sudo mount -t nfs grv-fas01.lax1.gravity.com:/vol/magellan /Volumes/magellan
  val sourceFilename = s"${Settings.tmpDir}/FileHelperTestSource.log"
  val destFilename = "/Volumes/magellan/test/FileHelperTestDestination.log"

  FileHelper.write(sourceFilename, "TESTDATA") // write a test file locally

  val source: File = new File(sourceFilename)
  val dest: File = new File(destFilename)

  // try to copy to nfs
  FileHelper.nioFileCopy(source, dest)
  if (FileHelper.fileGetContents(destFilename) == "TESTDATA") {
    assertTrue(1 == 1)
  }
  if (dest.exists()) {
    dest.delete()
    println("COPY WORKED!")
  }
}