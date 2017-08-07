package com.gravity.utilities.web
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.{FilterChain, FilterConfig, ServletRequest, ServletResponse}

/**
  * Created by robbie on 10/20/2016.
  *            _ 
  *          /  \
  *         / ..|\
  *        (_\  |_)
  *        /  \@'
  *       /     \
  *   _  /  \   |
  *  \\/  \  | _\
  *   \   /_ || \\_
  *    \____)|_) \_)
  *
  */
class BlockAccessToLoadBalancedRequestsFilter extends javax.servlet.Filter {
  import BlockAccessToLoadBalancedRequestsFilter._

  def init(filterConfig: FilterConfig): Unit = {}

  def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain): Unit = {
    receievedCounter.increment
    request match {
      case httpServletReq: HttpServletRequest => response match {
        case httpServletResp: HttpServletResponse =>
          Option(httpServletReq.getHeaderNames).foreach { headerNames =>
            var found = false
            while (headerNames.hasMoreElements && !found) {
              if (isLoadBalancedHeader(headerNames.nextElement())) {
                httpServletResp.setStatus(204)
                loadBalancerDetectedCounter.increment
                return chain.doFilter(request, httpServletResp)
              }
            }
          }

        case _ =>
          // nothing to see here
      }


      case _ =>
        // nothing to see here either
    }
    chain.doFilter(request, response)
  }

  def destroy(): Unit = {}
}

object BlockAccessToLoadBalancedRequestsFilter {
  import com.gravity.utilities.Counters._
  val loadBalancedHeaderNames: Set[String] = Set(
     "True-Client-IP"
    ,"X-Forwarded-For"
    ,"Grv-Client-IP"
  )

  def isLoadBalancedHeader(headerName: String): Boolean = loadBalancedHeaderNames.contains(headerName)

  val receievedCounter: PerSecondCounter = getOrMakePerSecondCounter( "BlockAccessToLoadBalancedRequestsFilter", "requests.received")
  val loadBalancerDetectedCounter: PerSecondCounter = getOrMakePerSecondCounter("BlockAccessToLoadBalancedRequestsFilter", "requests.loadbalance.detected")
}
