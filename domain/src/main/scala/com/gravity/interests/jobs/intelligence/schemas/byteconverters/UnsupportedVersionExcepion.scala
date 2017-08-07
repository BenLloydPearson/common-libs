package com.gravity.interests.jobs.intelligence.schemas.byteconverters

object UnsupportedVersionExcepion {
  def buildMessage(readName: String, unsupportVer: Integer, currentVer: Integer) =
    s"Cannot read $readName. Unsupported version: $unsupportVer whereas current version is: $currentVer"

  def buildMessage(readName: String, unsupportVer: Integer, minReadableVer: Integer, maxReadableVer: Integer) =
    s"Cannot read $readName. Unsupported version: $unsupportVer; must be between version $minReadableVer and version $maxReadableVer"
}
