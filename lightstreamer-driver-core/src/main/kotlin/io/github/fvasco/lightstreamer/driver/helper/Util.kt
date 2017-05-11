package io.github.fvasco.lightstreamer.driver.helper

import java.net.URLDecoder
import java.net.URLEncoder

internal fun String.urlEncode() = URLEncoder.encode(this, "UTF-8")
internal fun String.urlDecode() = URLDecoder.decode(this, "UTF-8")
