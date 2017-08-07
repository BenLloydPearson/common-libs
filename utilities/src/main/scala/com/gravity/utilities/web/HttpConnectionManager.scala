package com.gravity.utilities.web

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.{Charset, IllegalCharsetNameException, MalformedInputException, UnsupportedCharsetException}

import com.gravity.goose.Configuration
import com.gravity.goose.network.MaxBytesException
import com.gravity.logging.{CanLogstash, Logstashable}
import com.gravity.utilities.ScalaMagic._
import com.gravity.utilities._
import com.gravity.utilities.grvstrings._
import com.ibm.icu.text.CharsetDetector
import org.apache.http._
import org.apache.http.auth._
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods._
import org.apache.http.client.protocol.{HttpClientContext, RequestAcceptEncoding, ResponseContentEncoding}
import org.apache.http.config.{ConnectionConfig, RegistryBuilder, SocketConfig}
import org.apache.http.conn.ConnectionKeepAliveStrategy
import org.apache.http.conn.routing.HttpRoute
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client._
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.protocol._

import scala.collection._
import scala.io.Codec
import scalaz.syntax.std.option._

case class CouldNotParseException(message: String, attemptedCharset: String) extends Exception

case class HttpArgumentsOverrides(optCompress:            Option[Boolean] = None,
                                  optAuth:                Option[Option[BasicCredentials]] = None,
                                  optConnectionTimeout:   Option[Int] = None,
                                  optSocketTimeout:       Option[Int] = None,
                                  optUserAgent:           Option[String] = None,
                                  optDefaultMaxPerRoute:  Option[Int] = None) {
  def withOverrides(argsOverrides: Option[HttpArgumentsOverrides]): HttpArgumentsOverrides = argsOverrides match {
    case Some(over) => HttpArgumentsOverrides(
      optCompress           = over.optCompress.orElse(this.optCompress),
      optAuth               = over.optAuth.orElse(this.optAuth),
      optConnectionTimeout  = over.optConnectionTimeout.orElse(this.optConnectionTimeout),
      optSocketTimeout      = over.optSocketTimeout.orElse(this.optSocketTimeout),
      optUserAgent          = over.optUserAgent.orElse(this.optUserAgent),
      optDefaultMaxPerRoute = over.optDefaultMaxPerRoute.orElse(this.optDefaultMaxPerRoute)
    )

    case None => this
  }
}

case class HttpArguments(compress: Boolean = false,
                         auth: Option[BasicCredentials] = None,
                         connectionTimeout: Int = 120000,
                         socketTimeout: Int = 10000,
                         userAgent: String = "Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8",
                         defaultMaxPerRoute : Int = 500) {

  def withOverrides(argsOverrides: Option[HttpArgumentsOverrides]): HttpArguments = argsOverrides match {
    case Some(over) => HttpArguments(
      compress           = over.optCompress.getOrElse(this.compress),
      auth               = over.optAuth.getOrElse(this.auth),
      connectionTimeout  = over.optConnectionTimeout.getOrElse(this.connectionTimeout),
      socketTimeout      = over.optSocketTimeout.getOrElse(this.socketTimeout),
      userAgent          = over.optUserAgent.getOrElse(this.userAgent),
      defaultMaxPerRoute = over.optDefaultMaxPerRoute.getOrElse(this.defaultMaxPerRoute)
    )

    case None => this
  }

  /**
   * Returns an HttpArgumentsOverrides that overrides everything to match this HttpArguments.
   */
  def toHttpArgumentsOverrides: HttpArgumentsOverrides = HttpArgumentsOverrides(
    optCompress           = compress.some,
    optAuth               = Option(auth),
    optConnectionTimeout  = connectionTimeout.some,
    optSocketTimeout      = socketTimeout.some,
    optUserAgent          = userAgent.some,
    optDefaultMaxPerRoute = defaultMaxPerRoute.some)
}

object HttpArguments {
  // NOTE that defaults == new HttpArguments(), but that defaults != fromGooseConfiguration(new Configuration())

  val defaults: HttpArguments = new HttpArguments()

  def fromGooseConfiguration(config: Configuration): HttpArguments = {
    HttpArguments(connectionTimeout = config.getConnectionTimeout(), socketTimeout = config.getSocketTimeout(), userAgent = config.getBrowserUserAgent())
  }
}

trait HttpConnectionManager {
 import com.gravity.logging.Logging._

  object HttpMethods {
    val GET: String = HttpGet.METHOD_NAME
    val POST: String = HttpPost.METHOD_NAME
    val PUT: String = HttpPut.METHOD_NAME
    val HEAD: String = HttpHead.METHOD_NAME
    val DELETE: String = HttpDelete.METHOD_NAME
  }

  object Attributes {
    val PreemptiveAuth = "preemptive-auth"
  }

  val FragmentHash = '#'
  val QuestionMark = '?'
  val Ampersand = "&"
  val EqualSign = "="

  val clientBuilders: GrvConcurrentMap[HttpArguments, HttpClientBuilder] = new GrvConcurrentMap[HttpArguments, HttpClientBuilder]

  def getClientBuilder(args: HttpArguments = HttpArguments.defaults): HttpClientBuilder = {
    // Well, for Christ's sake...
    // http://stackoverflow.com/questions/25889925/apache-poolinghttpclientconnectionmanager-throwing-illegal-state-exception
    // clientBuilders.getOrElseUpdate(args, newClientBuilder(args))

    newClientBuilder(args)
  }

  private def newClientBuilder(args: HttpArguments = HttpArguments.defaults) = {
    import args._

    val socketConfig = SocketConfig.custom()
      .setTcpNoDelay(true)
      .setSoTimeout(socketTimeout)
      .build()

    val requestConfig = RequestConfig.custom()
      .setCookieSpec(CookieSpecs.BROWSER_COMPATIBILITY)
      .setConnectTimeout(connectionTimeout)
      .setStaleConnectionCheckEnabled(false)
      .build()

    val connectionConfig = ConnectionConfig.custom()
      .setCharset(Consts.UTF_8)
      .build()

    val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory]()
      .register("http", PlainConnectionSocketFactory.INSTANCE)
      .register("https", SSLConnectionSocketFactory.getSocketFactory)
      .build()

    val connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry)
    connectionManager.setMaxTotal(20000)
    connectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute)
    connectionManager.setMaxPerRoute(new HttpRoute(new HttpHost("localhost", 80)), 50)
    connectionManager.setDefaultConnectionConfig(connectionConfig)
    connectionManager.setDefaultSocketConfig(socketConfig)

    // Well, Crap: https://issues.apache.org/jira/browse/HTTPCLIENT-1577.
    val httpClientBuilder = HttpClients.custom()
      .setConnectionManager(connectionManager)
      .setDefaultRequestConfig(requestConfig)
//      .setDefaultConnectionConfig(connectionConfig)                     // Ignored! See "Well, Crap" above.
//      .setDefaultSocketConfig(socketConfig)                             // Ignored! See "Well, Crap" above.
      .setRetryHandler(new DefaultHttpRequestRetryHandler(0, false))
      .setUserAgent(userAgent)
      .setRedirectStrategy(new DefaultRedirectStrategy)
      .setKeepAliveStrategy(new ConnectionKeepAliveStrategy {
        def getKeepAliveDuration(p1: HttpResponse, p2: HttpContext): Long = {
          30 * 1000 //only keep alive for 30 seconds; trying to eliminate dead connection problem
        }
      })

    if (compress) {
      httpClientBuilder.addInterceptorLast(new RequestAcceptEncoding).addInterceptorLast(new ResponseContentEncoding)
    }

    auth match {
      case Some(c) =>
        val credentialsProvider = new BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, c.credentials)
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)

        // NOTE: Erik commented that he didn't immediately see why the following call is necessary,
        // since we are already setting a credential provider the normal way,
        // but INGEST fails to authenticate WordPress without it, so please keep it in.
        httpClientBuilder.addInterceptorFirst(BasicAuthRequestInterceptor)
      case None =>
    }

    httpClientBuilder
  }

  def getClient(args: HttpArguments = HttpArguments.defaults): CloseableHttpClient = getClientBuilder(args).build()

  private def getClientContext(args: HttpArguments): ClientContext = {
    val context = new BasicHttpContext()

    args.auth match {
      case Some(c) =>
        context.setAttribute(Attributes.PreemptiveAuth, new BasicScheme())

      case _ =>
    }

    ClientContext(getClient(args), context)
  }

  /**
   * Convenience for making an HTTP request and capturing the whole output as a string.
   */
  def execute(url: String, method: String = HttpMethods.GET, params: Map[String, String] = Map.empty,
              argsOverrides: Option[HttpArgumentsOverrides] = None,
              processor: RequestProcessor = RequestProcessor.empty, headers: Map[String, String] = Map.empty, maxBytes: Int = 0): HttpResult = {
    request(url, method, params, argsOverrides, processor, headers, maxBytes) { httpResultStream =>
      HttpResult(httpResultStream.status, httpResultStream.headers, httpResultStream.clientContext, httpResultStream.responseStream, httpResultStream.codec, maxBytes)
    }
  }

  /**
   * Make an HTTP request and handle the response stream.
   */
  def request[T](url: String, method: String = HttpMethods.GET,
                 params: Map[String, String] = Map.empty,
                 argsOverrides: Option[HttpArgumentsOverrides] = None,
                 processor: RequestProcessor = RequestProcessor.empty, headers: Map[String, String] = Map.empty, maxBytes: Int = 0)
                (handleResult: (HttpResultStream) => T): T = {
    val stream = open(url, method, params, argsOverrides, processor, headers)

    try {
      handleResult(stream)
    } finally {
      stream.close()
    }
  }

  /**
   * Lower-level way to open an HttpResultStream. The client must explicitly call .close() on the result.
   * You should probably use execute() or request() instead.
   */
  def open(url: String, method: String = HttpMethods.GET,
           params: Map[String, String] = Map.empty, argsOverrides: Option[HttpArgumentsOverrides] = None, processor: RequestProcessor = RequestProcessor.empty, headers: Map[String, String] = Map.empty): HttpResultStream = {
    // Note that some/none auth also comes in via argsOverrides.
    if (isNullOrEmpty(url)) throw new IllegalArgumentException("'url' must not be null or empty!")

    val args =  HttpArguments.defaults.withOverrides(argsOverrides)
//    GrvHttpParamsView.infoParamsSettings("com.gravity.utilities.web.HttpConnectionManager.open", args)

    val message = method.toUpperCase match {
      case HttpMethods.GET => new HttpGet(appendQueryParams(url, params))
      case HttpMethods.HEAD => new HttpHead(appendQueryParams(url, params))
      case HttpMethods.DELETE => new HttpDelete(appendQueryParams(url, params))
      case m =>
        val msg = m match {
          case HttpMethods.POST => new HttpPost(url)
          case HttpMethods.PUT => new HttpPut(url)
        }

        if (params.nonEmpty) {
          val entity = ParameterBasedEntity(params, processor)
          msg.setEntity(entity)
        }

        msg
    }

    for ((k,v) <- headers) message.addHeader(k, v)

    val clientContext = getClientContext(args)
    try {
      extractResult(clientContext, processor.requestMapper(message))
    }
    catch {
      case ex: Exception =>
        clientContext.close()
        throw ex
    }
  }

  private def appendQueryParams(originalUrl: String, params: Map[String, String]): String = {
    if (params.nonEmpty) {
      val sb = new StringBuilder
      sb.append(originalUrl)
      if (!originalUrl.contains(QuestionMark)) sb.append(QuestionMark) else sb.append(Ampersand)
      sb.append(params map { case (k, v) => k + EqualSign + v } mkString Ampersand)
      sb.toString()
    } else {
      originalUrl
    }
  }

  private def extractResult(clientContext: ClientContext, message: HttpRequestBase) = {
    val response = clientContext.client.execute(message, clientContext.context)
    val statusCode = if (response.getStatusLine != null) response.getStatusLine.getStatusCode else 500
    val (stream, codecOption) = if (response.getEntity != null) {
      val str = Some(response.getEntity.getContent)
      val enc = if (response.getEntity.getContentType != null) {
        val ctCharSet = try {
          ContentType.getOrDefault(response.getEntity).getCharset
        } catch {
          /*
           * Why the hell do I have to do this IllegalCharsetNameException fix-up, e.g. when encountering the following header?
               <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
           */
          case ex: IllegalCharsetNameException if ex.getCharsetName != null && ex.getCharsetName.toLowerCase.startsWith("charset=") =>
            Charset.forName(ex.getCharsetName.substring("charset=".length))

          case ex: UnsupportedCharsetException =>
            null
        }

        if (ctCharSet != null) {
          val ctCharSetName = ctCharSet.displayName()

          if (isNullOrEmpty(ctCharSetName))
            None
          else ctCharSetName.toUpperCase match {
            case "BINARY" => None
            case _ =>  Some(codecForString(ctCharSetName.toUpperCase))
          }
        } else None
      } else None
      (str, enc)
    } else (None, None)

    HttpResultStream(statusCode, response.getAllHeaders, clientContext, stream, codecOption)
  }

  protected def codecForString(encoding: String) = Codec(encoding)
}

object HttpConnectionManager extends HttpConnectionManager {
  val ordinaryUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36"
}

case class ClientContext(client: CloseableHttpClient, context: HttpContext) extends Closeable {
  override def close() {
    client.close()
  }
}

case class BasicCredentials(username: String, password: String) {
  lazy val credentials: Credentials = new UsernamePasswordCredentials(username, password)
}

case class HttpResultStream(status: Int, headers: Array[Header], clientContext: ClientContext,
                            responseStream: Option[InputStream], codec: Option[Codec] = None) extends Closeable {
  override def close() {
    try {
      responseStream foreach (_.close())
    }
    finally {
      clientContext.close()
    }
  }

  def hasContent: Boolean = responseStream.isDefined
}

object HttpResultStream {
  import com.gravity.logging.Logstashable._
  implicit val canLogStash: CanLogstash[HttpResultStream] with Object {def getKVs(t: HttpResultStream): scala.Seq[(String, String)]} = new CanLogstash[HttpResultStream] {
    override def getKVs(t: HttpResultStream): scala.Seq[(String, String)] = scala.Seq(
      Status -> t.status.toString,
      Headers -> t.headers.mkString("\r\n"),
      HasContent -> t.hasContent.toString,
      Logstashable.ClientContext -> t.clientContext.toString,
      ResponseStream -> t.responseStream.toString,
      Logstashable.Codec -> t.codec.toString
    )
  }
}

case class HttpResultStreamLite(status: Int, responseStream: Option[InputStream])

case class DetectionResult(text: String, charSetName: String, language: String, confidence: Int)

object HttpResult {
 import com.gravity.logging.Logging._
  val defaultCodec: Codec = Codec("UTF-8")

  def apply(status: Int, headers: Array[Header], clientContext: ClientContext, responseStream: Option[InputStream], codec: Option[Codec] = None, maxBytes : Int) : HttpResult = {
    new HttpResult(status, headers, clientContext, responseStream, codec, maxBytes)
  }

  def detectAndCovertToString(bytes: Array[Byte]): DetectionResult = {
    val detector = new CharsetDetector()
    detector.enableInputFilter(true)
    detector.setText(bytes)

    try {
      val charSetMatch = detector.detect()
      if(charSetMatch == null) throw new CouldNotParseException("Could not get a character set match", emptyString)
      if(charSetMatch.getConfidence < 15) throw new CouldNotParseException("Got character set match of " + charSetMatch.getName + " in language " + charSetMatch.getLanguage + ", but confidence was only " + charSetMatch.getConfidence, charSetMatch.getName)
      DetectionResult(charSetMatch.getString, charSetMatch.getName, charSetMatch.getLanguage, charSetMatch.getConfidence)
    }
    catch {
      case e: Exception =>
        e match {
          case cnpe:CouldNotParseException => throw cnpe
          case _ => throw new CouldNotParseException("Exception from detector " + e.toString, emptyString)}
    }
  }

  def convertStreamToString(is: InputStream, maxBytes: Int, knownCodec: Option[Codec]): String = {
    val byteBuf : Array[Byte] = new Array[Byte](2048)
    val byteStream = new ByteArrayOutputStream(10240)

    var lastRead = 0
    var bytesRead = 0
    do {
      lastRead = is.read(byteBuf)
      if(lastRead > 0) {
        bytesRead += lastRead
        if(maxBytes > 0 && bytesRead > maxBytes)
          throw new MaxBytesException
        byteStream.write(byteBuf, 0, lastRead)
      }
    } while (lastRead > 0)

    val everythingArray = byteStream.toByteArray
    byteStream.close()
    val everythingByteBuffer = ByteBuffer.wrap(everythingArray)
    knownCodec match {
      case Some(codec) =>
        try {
          val charBuffer = codec.decoder.decode(everythingByteBuffer)
          charBuffer.toString
        }
        catch {
          case m:MalformedInputException =>
            val result = detectAndCovertToString(everythingArray)
            info("Decoding with supplied codec " + codec.name + " failed; detection found and used " + result.charSetName + " in language " + result.language + " with confidence " + result.confidence)
            result.text
        }
      case None =>
        //first try the default
        try {
          defaultCodec.decoder.decode(everythingByteBuffer).toString
        }
        catch {
          case m:MalformedInputException =>
            val result = detectAndCovertToString(everythingArray)
            info("Decoding with default codec " + defaultCodec.name + " failed; detection found and used " + result.charSetName + " in language " + result.language + " with confidence " + result.confidence)
            result.text
        }
    }
  }
}

class HttpResult(override val status: Int, override val headers: Array[Header], override val clientContext: ClientContext, override val responseStream: Option[InputStream], override val codec: Option[Codec] = None, maxBytes : Int) extends HttpResultStream(status, headers, clientContext, responseStream, codec) {
  val getContent: String = {
    responseStream match {
      case Some(is) =>
        HttpResult.convertStreamToString(is, maxBytes, codec)
        //        else
        //          Source.fromInputStream(is)(codec) mkString (emptyString)
      case None => grvstrings.emptyString
    }
  }
}

case class RequestProcessor(preParamsFx: (Map[String, String]) => Seq[(String, String)],
                            onEachParam: (String, String) => Unit,
                            postEntityStringFx: (String) => String,
                            requestMapper: HttpRequestBase => HttpRequestBase)

object RequestProcessor {
  val empty: RequestProcessor = RequestProcessor((m: Map[String, String]) => m.toSeq, (k: String, v: String) => {}, (s: String) => s, r => r)

  def entitySetter(entityStr: String): RequestProcessor = empty.copy(requestMapper = _ match {
    case httpPost: HttpPost =>
      httpPost.setEntity(new StringEntity(entityStr))
      httpPost

    case httpPut: HttpPut =>
      httpPut.setEntity(new StringEntity(entityStr))
      httpPut

    case other => other
  })
}

object BasicAuthRequestInterceptor extends HttpRequestInterceptor {
  def process(request: HttpRequest, context: HttpContext) {
    val authState = context.getAttribute(HttpClientContext.TARGET_AUTH_STATE).asInstanceOf[AuthState]
    // If no auth scheme available yet, try to initialize it pre-emptively
    if (authState.getAuthScheme == null) {
      val authScheme = context.getAttribute(HttpConnectionManager.Attributes.PreemptiveAuth).asInstanceOf[AuthScheme]
      val credsProvider = context.getAttribute(HttpClientContext.CREDS_PROVIDER).asInstanceOf[CredentialsProvider]
      val targetHost = context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST).asInstanceOf[HttpHost]
      if (authScheme != null) {
        val creds = credsProvider.getCredentials(new AuthScope(targetHost.getHostName, targetHost.getPort))
        if (creds == null) {
          throw new HttpException("No credentials for preemptive authentication")
        }
        authState.update(authScheme, creds)
      }
    }
  }
}

object RequestLengthApp extends App {
//  GrvHtmlFetcher.getHtml(new Configuration, "http://yes.evolucao.tw/Coringa4i20").getOrElse(emptyString)
//  GrvHtmlFetcher.getHtml(new Configuration, "http://tilefloorheater.apoluti.com/floor-heat-mat-120vac-3ft-x-7ft-great-price/").getOrElse(emptyString)
//  GrvHtmlFetcher.getHtml(new Configuration, "http://instagram.com/p/Q76AG8H0Yy/").getOrElse(emptyString)
//  GrvHtmlFetcher.getHtml(new Configuration, "http://www.google.com").getOrElse(emptyString)
  GrvHtmlFetcher.getHtml(new Configuration, "http://www.feriadellibro.cultura.df.gob.mx/index.php/3chpc").getOrElse(emptyString)
}
