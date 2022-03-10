package io.parapet.net

object Exceptions {
   case class TimeoutException(message: String, cause: Throwable = null)
     extends RuntimeException(message, cause)


}
