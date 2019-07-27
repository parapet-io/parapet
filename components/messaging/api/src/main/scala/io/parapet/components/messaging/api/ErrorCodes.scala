package io.parapet.components.messaging.api

object ErrorCodes {

  val basicErrorCode = 100

  val UnknownError: Int                            = basicErrorCode + 0
  val TransferError: Int                           = basicErrorCode + 1
  val EncodingError: Int                           = basicErrorCode + 2
  val EventHandlingError: Int                      = basicErrorCode + 3
  val RequestExpiredError: Int                     = basicErrorCode + 4
}