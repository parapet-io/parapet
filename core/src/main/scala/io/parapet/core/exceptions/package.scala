package io.parapet.core

package object exceptions {

  class UnknownProcessException(message: String) extends RuntimeException(message)

  class EventDeliveryException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)

  class EventRecoveryException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)

}
