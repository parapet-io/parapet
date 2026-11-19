package io.parapet.journal.effect

import io.parapet.Codec

final case class FailureData(
    exceptionType: String,
    message: Option[String],
    stackTrace: List[StackTraceElement],
    cause: Option[FailureData] // enum (recorded, none, omitted)
) derives Codec
