package io.parapet.core.annotations

/** Marks a symbol as part of the runtime's developer-facing API.
  *
  * Such symbols are exposed for advanced extension scenarios (custom interpreters, schedulers, transports) but are not
  * stable for end-user code; signatures may change between releases.
  *
  * @param description
  *   optional explanation of the intended use case.
  */
class developerApi(description: String = "") extends scala.annotation.StaticAnnotation
