package io.parapet.core

import io.parapet.ProcessRef

package object exceptions {

  case class UnknownProcessException(message: String) extends RuntimeException(message)

  object UnknownProcessException {
    def apply(ref: ProcessRef): UnknownProcessException = {
      new UnknownProcessException(s"process: '$ref' doesn't exist")
    }
  }

  case class EventHandlingException(message: String = "", cause: Throwable = null)
      extends RuntimeException(message, cause)

  case class EventDeliveryException(message: String = "", cause: Throwable = null)
      extends RuntimeException(message, cause)

  case class EventQueueIsFullException(message: String) extends RuntimeException(message)

  case class EventMatchException(message: String) extends RuntimeException(message)

  case class UninitializedProcessException(message: String) extends RuntimeException(message)

  case class ProcessStoppedException(message: String) extends RuntimeException(message)

  object ProcessStoppedException {
    def apply(ref: ProcessRef): ProcessStoppedException = {
      new ProcessStoppedException(s"process: '$ref' is already stopped")
    }
  }

}
