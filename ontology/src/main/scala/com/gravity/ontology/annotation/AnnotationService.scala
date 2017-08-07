package com.gravity.ontology.annotation

import org.openrdf.rio.{RDFFormat, RDFWriter, Rio}
import com.gravity.ontology.vocab.NS
import java.util.UUID
import org.apache.commons.codec.digest.DigestUtils
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import java.io.{File, FileOutputStream}
import org.openrdf.model.impl._
import org.openrdf.model._
import org.joda.time.DateTime
import java.math.BigInteger
import java.lang.management.ManagementFactory

/**
 * Methods for saving user ontology annotations.
 */
trait AnnotationService extends AnnotationQuery {
  this: RDFWriterFactory =>

  private val YMD_FORMAT = DateTimeFormat.forPattern("YYYY/MM/dd")

  private implicit def str2uri(str: String) = new URIImpl(str)
  private implicit def writerAbbr(w: RDFWriter) = (subject: Resource, predicate: URI, `object`: Value) => w.handleStatement(new StatementImpl(subject, predicate, `object`))

  def flagBadTopic(topicUri: URI, suggestion: AnnotatorSuggestion, reason: String)(implicit commonParams: AnnotationService.CommonParams) = {
    createAnnotation(commonParams) { (writer, annotationUri) =>
      writer(annotationUri, NS.MATCHED_TOPIC, topicUri)
      writer(annotationUri, suggestion.annotationPredicate, suggestion.annotationObject)
      writer(annotationUri, NS.ANNOTATION_REASON, new LiteralImpl(reason))
    }
  }

  def renameTopic(topicUri: URI, label: String)(implicit commonParams: AnnotationService.CommonParams) = {
    createAnnotation(commonParams) { (writer, annotationUri) =>
      writer(annotationUri, NS.MATCHED_TOPIC, topicUri)
      writer(annotationUri, NS.SHOULD_RENAME, new LiteralImpl(label))
    }
  }

  def correctTopic(topicUri: URI, phrases: Iterable[AnnotatedPhrase], shouldMatchTopicUri: URI = NS.NO_SUGGESTION)(implicit commonParams: AnnotationService.CommonParams) = {
    createAnnotation(commonParams) { (writer, annotationUri) =>
      writer(annotationUri, NS.MATCHED_TOPIC, topicUri)
      writer(annotationUri, NS.SHOULD_MATCH_TOPIC, shouldMatchTopicUri)
      phrases foreach { phrase =>
        val annotatedPhraseUri = uniqueAnnotatedPhraseUri
        writer(annotationUri, NS.MACHINE_SELECTED_PHRASE, annotatedPhraseUri)
        writer(annotatedPhraseUri, NS.NGRAM, new LiteralImpl(phrase.phrase))
        writer(annotatedPhraseUri, NS.AT_INDEX, new IntegerLiteralImpl(BigInteger.valueOf(phrase.atIndex)))
      }
    }
  }

  def correctConceptPath(conceptUris: Seq[URI], badConceptUri: URI, shouldMatchConceptUri: URI = NS.NO_SUGGESTION)(implicit commonParams: AnnotationService.CommonParams) = {
    createAnnotation(commonParams) { (writer, annotationUri) =>
      var conceptLink = uniqueConceptLinkUri
      writer(annotationUri, NS.BROADER_PROPOSED_CONCEPT, conceptLink)

      conceptUris foreach { conceptUri =>
        writer(conceptLink, NS.MATCHED_CONCEPT, conceptUri)
        if (conceptUri == badConceptUri) {
          writer(conceptLink, NS.SHOULD_MATCH_CONCEPT, shouldMatchConceptUri)
        }

        if (conceptUri != conceptUris.last) {
          val nextLink = uniqueConceptLinkUri
          writer(conceptLink, NS.BROADER_PROPOSED_CONCEPT, nextLink)
          conceptLink = nextLink
        }
      }
    }
  }

  private def createAnnotation(commonParams: AnnotationService.CommonParams)(f: (RDFWriter, URI) => Any) = {
    implicit val atTime = commonParams.atTime.getOrElse(new DateTime)
    val annotationUri = uniqueAnnotationUri(commonParams.atUrl)

    val writer = writerFactory()
    writer.startRDF()

    writer(annotationUri, NS.BY_USER, NS.GRAVITY_ANNOTATION_USER_NAMESPACE + commonParams.sid)
    writer(annotationUri, NS.URL, commonParams.atUrl)
    writer(annotationUri, NS.AT_TIME, vf.createLiteral(atTime))
    commonParams.siteGuid foreach { siteGuid =>
      writer(annotationUri, NS.SITEGUID, new LiteralImpl(siteGuid))
    }
    commonParams.shouldApplyTo foreach { shouldApplyTo =>
      writer(annotationUri, NS.SHOULD_APPLY_TO, new LiteralImpl(shouldApplyTo))
    }

    f(writer, annotationUri)
    writer.endRDF()
    annotationUri
  }

  private def uniqueAnnotationUri(url: String)(implicit atTime: DateTime) = {
    val ymd = YMD_FORMAT.print(atTime)
    val tinyurlhash = DigestUtils.md5Hex (url) take (6)
    val uuid = UUID.randomUUID
    val uri = NS.GRAVITY_ANNOTATION_NAMESPACE + ymd + "/" + tinyurlhash + "/" + uuid
    new URIImpl(uri)
  }

  private def uniqueConceptLinkUri = {
    new URIImpl(NS.GRAVITY_ANNOTATION_NAMESPACE + "conceptlink/" + UUID.randomUUID)
  }

  private def uniqueAnnotatedPhraseUri(implicit commonParams: AnnotationService.CommonParams) = {
    implicit val atTime = commonParams.atTime.getOrElse(new DateTime)
    val annotationUri = uniqueAnnotationUri(commonParams.atUrl).stringValue
    annotationUri replace (NS.GRAVITY_ANNOTATION_NAMESPACE, NS.GRAVITY_ANNOTATION_PHRASE_NAMESPACE)
  }

  /**
   * Ensures annotations made with the {@AnnotationService} passed to the closure `f` will be contained in a single log.
   */
  def withSingleLog(f: (AnnotationService) => Set[URI])(implicit commonParams: AnnotationService.CommonParams) = {
    val writer = writerFactory()
    writer.startRDF()
    val noopStartAndEndService = new AnnotationService with RDFWriterFactory {
      override val writerFactory = () => new ForwardingRDFWriter(writer) {
        override def startRDF() {}
        override def endRDF() {}
      }
    }

    val writtenUris = f(noopStartAndEndService)
    writer.endRDF()
    writtenUris
  }
}
object AnnotationService {
  case class CommonParams(
    sid: String,
    atUrl: String,
    atTime: Option[DateTime] = None,
    siteGuid: Option[String] = None,
    shouldApplyTo: Option[String] = None
  )
}

trait RDFWriterFactory {
  val writerFactory: () => RDFWriter
}

trait HabitatBackupsRDFWriterFactory extends RDFWriterFactory {
  val ANNOTATION_LOG_DIR = "/mnt/habitat_backups/writable/annotation"
  private val YMD_FOLDER_FORMAT = DateTimeFormat.forPattern("YYYYMMdd")

  override val writerFactory = () => {
    val now = new DateTime
    val dir = new File(ANNOTATION_LOG_DIR + "/" + YMD_FOLDER_FORMAT.print(now) + "/")
    dir.mkdirs()

    val pid = ManagementFactory.getRuntimeMXBean.getName
    val ts = ISODateTimeFormat.dateTimeNoMillis.print(now)
    val uuid = UUID.randomUUID()
    val ntfile = new File(dir, ts + "." + pid + "." + uuid + ".nt")

    val out = new FileOutputStream(ntfile)
    
    new ForwardingRDFWriter(Rio.createWriter(RDFFormat.NTRIPLES, out)) {
      override def endRDF() {
        delegate.endRDF()
        out.close()
      }
    }
  }
}

object WritingAnnotationService extends AnnotationService with HabitatBackupsRDFWriterFactory