package io.parapet.core.annotations

/** Marks a symbol as experimental: it may change shape, semantics, or disappear entirely in a future release. Use with
  * caution in long-lived code.
  *
  * @param description
  *   optional notes about what is unstable.
  */
class experimental(description: String = "") extends scala.annotation.StaticAnnotation
