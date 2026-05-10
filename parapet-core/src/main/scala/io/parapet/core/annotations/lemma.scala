package io.parapet.core.annotations

import io.parapet.core.doc.Lemma

/** Documents that a symbol implements (or relies on) a particular [[Lemma]] from the design notes.
  *
  * Useful for traceability between code and the formal/semi-formal reasoning supporting it. Has no runtime semantics;
  * purely a documentation artifact picked up by ScalaDoc and tooling.
  *
  * @param name
  *   optional short identifier of the lemma.
  * @tparam T
  *   the [[Lemma]] subtype this annotation references.
  */
class lemma[T <: Lemma](name: String = "") extends scala.annotation.StaticAnnotation {

  /** Convenience overload accepting the lemma's runtime [[Class]] instead of a name. */
  def this(clazz: Class[T]) = this("")
}
