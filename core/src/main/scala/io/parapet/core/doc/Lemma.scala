package io.parapet.core.doc

/** Marker for a documented invariant or correctness claim referenced from code via the
  * [[io.parapet.core.annotations.lemma]] annotation.
  *
  * Subclasses describe the lemma in their [[description]] and act as compile-time witnesses that the lemma exists.
  * There is no runtime enforcement.
  */
trait Lemma {

  /** Human-readable explanation of the lemma. */
  val description: String

}
