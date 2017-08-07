package com.gravity.textutils.analysis.lingpipe

import com.aliasi.chunk.{Chunk, ChunkFactory}
import com.google.common.collect.ForwardingObject
import com.aliasi.coref.Mention

abstract class ForwardingChunk(chunk: Chunk) extends ForwardingObject with Chunk {
  override def delegate: Chunk = chunk
  override def start = delegate.start
  override def end = delegate.end
  override def `type` = delegate.`type`
  override def score = delegate.score
}

case class PlainChunk(chunk: Chunk) extends ForwardingChunk(chunk) {
  override def `type` = PlainChunk.PLAIN_CHUNK_TYPE
}
object PlainChunk {
  val PLAIN_CHUNK_TYPE = ChunkFactory.createChunk(0, 1).`type`
}

case class MentionChunk(chunk: Chunk, entityId: Int, mention: Mention) extends ForwardingChunk(chunk)