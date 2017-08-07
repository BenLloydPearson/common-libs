package com.gravity.utilities.web

import org.apache.http.HttpException

class PartnerFeedException(message : String) extends HttpException(message)