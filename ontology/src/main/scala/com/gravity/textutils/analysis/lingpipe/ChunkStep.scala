package com.gravity.textutils.analysis.lingpipe

import com.aliasi.chunk.ChunkAndCharSeq

case class ChunkInSentence(chunk: ChunkAndCharSeq, sentenceIdx: Int)

trait ChunkStep {
  type ChunkAcc = Seq[ChunkInSentence]
  
  def apply(acc: ChunkAcc, chunkedSentences: Seq[ChunkAndCharSeq], originalText: CharSequence): ChunkAcc
}